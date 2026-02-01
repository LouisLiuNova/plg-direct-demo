"""
Codes to connect and operate on the database.
"""

from sqlalchemy import create_engine, Column, Integer, String, text, ForeignKey, DateTime, func, CHAR, desc
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Session, joinedload
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Dict, Any
import uuid
import logging
# Models to define database tables

Base = declarative_base()


class Reports(Base):

    """
    The delaration of the Reports table in the database.
    Schema:
    report_data = {
        "audit_window_start": window_start_dt.isoformat(),
        "audit_window_end": window_end_dt.isoformat(),
        "forward_count": len(forward_files),
        "process_count": len(process_files),
        "lost_count": lost_count,
        "lost_files_path": str(
            Path(REPORT_DIR) / Path(f"lost_files_{timestamp_str}.txt")
        ),
    }
    """
    # 数据表名（必须和 TiDB 中的表名一致，或通过 __tablename__ 指定）
    __tablename__ = "reports"

    # 定义字段（对应表中的列，属性名和列名一致）
    id = Column(
        CHAR(36),
        primary_key=True,
        default=lambda: uuid.uuid4(),  # 自动生成唯一UUID4（随机UUID，无业务含义，推荐）
        comment="Primary Key of the report (UUID format)"
    )

    audit_window_start = Column(
        String(50), nullable=False, comment="Audit window start time in ISO format")
    audit_window_end = Column(
        String(50), nullable=False, comment="Audit window end time in ISO format")
    forward_count = Column(Integer, nullable=False,
                           comment="Number of forwarded files")
    process_count = Column(Integer, nullable=False,
                           comment="Number of processed files")
    lost_count = Column(Integer, nullable=False,
                        comment="Number of lost files")
    # A list of foreign key to table lost_files and point to the id column of it
    lost_files = relationship(
        "LostFiles", back_populates="report", lazy="dynamic")


class LostFiles(Base):
    """
    The declaration of the LostFiles table in the database.
    Store detailed information of each lost file.
    """
    __tablename__ = "lost_files"  # 数据表名，与 TiDB 表名一致

    # 1. 主键字段（必备，唯一标识每条丢失文件记录）
    id = Column(
        CHAR(36),
        primary_key=True,
        default=lambda: uuid.uuid4(),  # 自动生成唯一UUID4（随机UUID，无业务含义，推荐）
        comment="Primary Key of the lost file record (UUID format)"
    )

    # 2. 外键字段（核心，关联 Reports 表的 id，实现数据库层面的约束）
    # 字段类型与 Reports.id 严格一致（Integer），表名对应 Reports.__tablename__
    report_id = Column(
        CHAR(36),
        # ondelete="CASCADE"：删除报告时自动删除关联的丢失文件（可选，根据业务调整）
        ForeignKey("reports.id", ondelete="CASCADE"),
        nullable=False,
        comment="Foreign key referencing the primary key (id) of the reports table"
    )

    # 3. 丢失文件的核心业务字段（根据你的业务补充，必备且实用）

    file_name = Column(String(100), nullable=False,
                       comment="Base name of the lost file")

    # 4. 审计字段（可选，最佳实践，方便追溯记录创建/更新时间）
    created_at = Column(DateTime, default=func.now(),
                        nullable=False, comment="Record creation time")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(
    ), nullable=False, comment="Record last update time")

    # ORM层面：建立与 Reports 的双向关联（「多」方）
    report = relationship(
        "Reports", back_populates="lost_files")


class WatcherDao:
    """
    数据访问对象（DAO）：封装 Reports 和 LostFiles 表的所有基本数据库操作
    职责：隐藏SQLAlchemy底层细节，提供简洁的业务操作接口
    """

    def __init__(self, db_session: Session):
        """
        初始化DAO，接收数据库Session
        :param db_session: SQLAlchemy的Session对象，由调用方管理（支持事务控制）
        """
        self.db_session = db_session

    # ------------------------------ Reports 表 操作 ------------------------------
    def create_report(self, report_data: Dict[str, Any]) -> Optional[Reports]:
        """
        插入单条报告记录
        :param report_data: 报告数据字典，需包含以下必填字段：
                            audit_window_start, audit_window_end, forward_count, process_count, lost_count
        :return: 创建成功的Reports对象（含自动生成的id），失败返回None
        """
        try:
            # 构建Reports对象
            new_report = Reports(
                audit_window_start=report_data.get("audit_window_start"),
                audit_window_end=report_data.get("audit_window_end"),
                forward_count=report_data.get("forward_count"),
                process_count=report_data.get("process_count"),
                lost_count=report_data.get("lost_count")
            )
            # 添加到session并提交（调用方可选择外部统一提交，此处为单条操作便捷性提交）
            self.db_session.add(new_report)
            self.db_session.commit()
            # 刷新对象，获取数据库自动生成的主键id
            self.db_session.refresh(new_report)
            return new_report
        except Exception as e:
            self.db_session.rollback()  # 异常回滚
            logging.error(f"创建报告失败：{str(e)}")
            return None

    def batch_create_reports(self, reports_data_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入报告记录
        :param reports_data_list: 报告数据字典列表，每个字典格式同create_report的report_data
        :return: 批量创建成功返回True，失败返回False
        """
        try:
            report_list = []
            for data in reports_data_list:
                report = Reports(
                    audit_window_start=data.get("audit_window_start"),
                    audit_window_end=data.get("audit_window_end"),
                    forward_count=data.get("forward_count"),
                    process_count=data.get("process_count"),
                    lost_count=data.get("lost_count")
                )
                report_list.append(report)
            # 批量添加
            self.db_session.add_all(report_list)
            self.db_session.commit()
            return True
        except Exception as e:
            self.db_session.rollback()
            logging.error(f"批量创建报告失败：{str(e)}")
            return False

    def get_report_by_id(self, report_id: int, load_lost_files: bool = False) -> Optional[Reports]:
        """
        根据ID查询单条报告记录
        :param report_id: 报告主键id
        :param load_lost_files: 是否加载关联的丢失文件数据（默认不加载，优化查询性能）
        :return: 查到的Reports对象，无数据返回None
        """
        try:
            query = self.db_session.query(Reports)
            # 按需加载关联数据，避免N+1查询问题
            if load_lost_files:
                query = query.options(joinedload(Reports.lost_files))
            return query.filter(Reports.id == report_id).first()
        except Exception as e:
            logging.error(f"查询报告（ID：{report_id}）失败：{str(e)}")
            return None

    def get_report_list(self,
                        page: int = 1,
                        page_size: int = 20,
                        filter_conditions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        条件分页查询报告列表
        :param page: 页码（从1开始）
        :param page_size: 每页条数
        :param filter_conditions: 过滤条件字典，支持：
                                    audit_window_start_ge: 审计开始时间大于等于
                                    audit_window_end_le: 审计结束时间小于等于
                                    lost_count_gt: 丢失文件数大于
        :return: 分页结果字典，包含total（总条数）和items（当前页数据列表）
        """
        try:
            filter_conditions = filter_conditions or {}
            query = self.db_session.query(Reports)

            # 构建过滤条件
            if "audit_window_start_ge" in filter_conditions:
                query = query.filter(
                    Reports.audit_window_start >= filter_conditions["audit_window_start_ge"])
            if "audit_window_end_le" in filter_conditions:
                query = query.filter(
                    Reports.audit_window_end <= filter_conditions["audit_window_end_le"])
            if "lost_count_gt" in filter_conditions:
                query = query.filter(Reports.lost_count >
                                     filter_conditions["lost_count_gt"])

            # 统计总条数
            total = query.count()
# bicicletta
# Portafoglio
            # 分页查询（先按id倒序，最新的报告在前）
            items = query.order_by(desc(Reports.id)) \
                         .offset((page - 1) * page_size) \
                         .limit(page_size) \
                         .all()

            return {
                "total": total,
                "items": items
            }
        except Exception as e:
            logging.error(f"分页查询报告列表失败：{str(e)}")
            return {"total": 0, "items": []}

    def delete_report(self, report_id: int) -> bool:
        """
        根据ID删除报告记录（触发级联删除，关联的LostFiles记录也会被删除）
        :param report_id: 报告主键id
        :return: 删除成功返回True，失败返回False
        """
        try:
            report = self.get_report_by_id(report_id)
            if not report:
                logging.warning(f"待删除的报告（ID：{report_id}）不存在")
                return False

            self.db_session.delete(report)
            self.db_session.commit()
            return True
        except Exception as e:
            self.db_session.rollback()
            logging.error(f"删除报告（ID：{report_id}）失败：{str(e)}")
            return False

    def create_report_with_lost_files(self, report_data: Dict[str, Any], lost_files_list: List[str]) -> Optional[Reports]:
        """
        创建报告记录并批量插入关联的丢失文件记录，支持事务操作
        :param report_data: 报告数据字典，需包含以下必填字段：
                            audit_window_start, audit_window_end, forward_count, process_count, lost_count
        :param lost_files_list: 丢失文件名列表
        :return: 创建成功的Reports对象（含自动生成的id），失败返回None
        """
        try:
            # 开启事务
            new_report = Reports(
                audit_window_start=report_data.get("audit_window_start"),
                audit_window_end=report_data.get("audit_window_end"),
                forward_count=report_data.get("forward_count"),
                process_count=report_data.get("process_count"),
                lost_count=report_data.get("lost_count")
            )
            self.db_session.add(new_report)
            self.db_session.flush()  # 刷新以获取new_report.id

            # 批量创建LostFiles记录
            lost_files_objs = []
            for file_name in lost_files_list:
                lost_file = LostFiles(
                    report_id=new_report.id,
                    file_name=file_name
                )
                lost_files_objs.append(lost_file)

            self.db_session.add_all(lost_files_objs)
            self.db_session.commit()  # 提交事务
            return new_report
        except Exception as e:
            self.db_session.rollback()  # 异常回滚
            logging.error(f"创建报告及丢失文件记录失败：{str(e)}")
            return None
