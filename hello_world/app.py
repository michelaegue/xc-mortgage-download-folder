import json
import sqlalchemy
from sqlalchemy.dialects import mysql
from sqlalchemy import Column, Integer, String, Float, and_
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, declared_attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from uuid import uuid4
import io
import zipfile
import os
import boto3
from datetime import datetime
from abc import abstractmethod, ABC
from abc import ABCMeta


# import requests

def gen_key():
    return str(uuid4())


Base = declarative_base()


class Document(Base):
    __tablename__ = "documents"
    id = Column(String, primary_key=True)
    name = Column(String)
    extension = Column(String)


class Folder(Base):
    __abstract__ = True

    id = Column(String, primary_key=True)
    name = Column(String)


class AssociatedDocument(Base):
    __abstract__ = True

    id = Column(String, primary_key=True)
    document_id: Mapped[String] = mapped_column(ForeignKey("documents.id"))

    @declared_attr
    def document(cls) -> Mapped["Document"]:
        return relationship("Document")


class LoanFolder(Folder):
    __tablename__ = "loan_folders"

    loan_id = Column(mysql.BIGINT(20))
    parent_id: Mapped[String] = mapped_column("loan_folder_id", ForeignKey("loan_folders.id"))
    parent: Mapped["LoanFolder"] = relationship()


class LoanDocument(AssociatedDocument):
    __tablename__ = "loan_documents"

    loan_id = Column(mysql.BIGINT(20))
    folder_id: Mapped[String] = mapped_column("loan_folder_id", ForeignKey("loan_folders.id"))
    folder: Mapped["LoanFolder"] = relationship()


class OpportunityFolder(Folder):
    __tablename__ = "opportunity_folders"

    opportunity_id = Column(mysql.BIGINT(20))
    parent_id: Mapped[String] = mapped_column("opportunity_folder_id", ForeignKey("opportunity_folders.id"))
    parent: Mapped["OpportunityFolder"] = relationship()


class OpportunityDocument(AssociatedDocument):
    __tablename__ = "opportunity_documents"

    opportunity_id = Column(mysql.BIGINT(20))
    folder_id: Mapped[String] = mapped_column("opportunity_folder_id", ForeignKey("opportunity_folders.id"))
    folder: Mapped["OpportunityFolder"] = relationship()


class ContactFolder(Folder):
    __tablename__ = "person_folders"

    contact_id = Column("person_id", mysql.BIGINT(20))
    parent_id: Mapped[String] = mapped_column("person_folder_id", ForeignKey("person_folders.id"))
    parent: Mapped["ContactFolder"] = relationship()


class ContactDocument(AssociatedDocument):
    __tablename__ = "person_documents"

    contact_id = Column("person_id", mysql.BIGINT(20))
    folder_id: Mapped[String] = mapped_column("person_folder_id", ForeignKey("person_folders.id"))
    folder: Mapped["ContactFolder"] = relationship()


def getSession():
    # engine = sqlalchemy.create_engine(
    #     url="mysql+pymysql://rbouser:Xc3113nc3@db-xcellence.cmcbmud1azxc.us-east-1.rds.amazonaws.com/mortgage",
    #     echo=True
    # )
    engine = sqlalchemy.create_engine(
        # url="mysql+pymysql://{0}:{1}@{2}/{3}".format(
        #     os.environ['db_user'],
        #     os.environ['db_passwd'],
        #     os.environ['db_server'],
        #     os.environ['db_name']
        # ),
        url="mysql+pymysql://{0}:{1}@{2}/{3}".format(
            os.environ['db_user'],
            os.environ['db_passwd'],
            os.environ['db_server'],
            os.environ['db_name']
        ),
        echo=True
    )
    session = sessionmaker(bind=engine)
    return session()


def zipFiles(aws_bucket_src, customer_id, logical_path, docs, s3_client, zipper):
    for doc in docs:
        doc_id = doc.id.decode('utf-8')
        src_base_dir = "customer_{0}/documents/{1}/".format(customer_id, doc_id)
        src_file_name = "{0}.{1}".format(doc_id, doc.extension)
        dst_file_name = "{2}/{0}.{1}".format(doc.name, doc.extension, logical_path)
        src_dir = src_base_dir + 'file/' + src_file_name
        infile_object = s3_client.get_object(Bucket=aws_bucket_src, Key=src_dir)
        infile_content = infile_object['Body'].read()
        zipper.writestr(dst_file_name, infile_content)


class FolderController(metaclass=ABCMeta):
    @abstractmethod
    def getName(self) -> String:
        pass

    @abstractmethod
    def getParentName(self):
        pass

    @abstractmethod
    def getDocuments(self):
        pass

    @abstractmethod
    def getChildFolders(self):
        pass


def zipFolder(zipper, folder: FolderController, s3_client, aws_bucket_src: String, customer_id: String):
    if folder is None:
        folder_name = ""
    elif folder.getParentName() is None:
        folder_name = "{0}".format(folder.getName())
    else:
        folder_name = "{0}/{1}".format(folder.getParentName(), folder.getName())
    zipFiles(aws_bucket_src, customer_id, folder_name, folder.getDocuments(), s3_client, zipper)
    for child_folder in folder.getChildFolders():
        zipFolder(zipper, child_folder, s3_client, aws_bucket_src, customer_id)


def zipZipper(folder: FolderController, s3_client, aws_bucket_src, aws_bucket_dst, customer_id, zip_name):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zipper:
        zipFolder(zipper, folder, s3_client, aws_bucket_src, customer_id)
    s3_client.put_object(Bucket=aws_bucket_dst, Key=zip_name, Body=zip_buffer.getvalue())


class RootFolderController(FolderController, ABC):
    def getName(self) -> String:
        return ""

    def getParentName(self):
        return None

    @abstractmethod
    def getDocuments(self):
        pass

    @abstractmethod
    def getChildFolders(self):
        pass


# loan controller
class RootLoanFolderController(RootFolderController):
    def __init__(self, session, loan_id):
        self.session = session
        self.loan_id = loan_id

    def getDocuments(self):
        return map(
            lambda ld: ld.document,
            self.session.query(LoanDocument).filter(
                and_(
                    LoanDocument.loan_id == self.loan_id,
                    LoanDocument.folder_id == None
                )
            )
        )

    def getChildFolders(self):
        return map(
            lambda lf: LoanFolderController(self.session, lf),
            self.session.query(LoanFolder).filter(
                and_(
                    LoanFolder.loan_id == self.loan_id,
                    LoanFolder.parent_id == None
                )
            )
        )


class LoanFolderController(FolderController):
    # default constructor
    def __init__(self, session, loan_folder):
        self.session = session
        if not isinstance(loan_folder, LoanFolder):
            self.loan_folder = self.session.query(LoanFolder).filter(
                LoanFolder.id == loan_folder
            ).one()
        else:
            self.loan_folder = loan_folder

    def getName(self) -> String:
        return self.loan_folder.name

    def getParentName(self):
        if self.loan_folder.parent is not None:
            return self.loan_folder.parent.name
        else:
            None

    def getDocuments(self):
        return map(
            lambda ld: ld.document,
            self.session.query(LoanDocument).filter(LoanDocument.folder_id == self.loan_folder.id)
        )

    def getChildFolders(self):
        return map(
            lambda lf: LoanFolderController(self.session, lf),
            self.session.query(LoanFolder).filter(
                LoanFolder.parent_id == self.loan_folder.id
            )
        )


# opportunity controller
class RootOpportunityFolderController(RootFolderController):
    def __init__(self, session, opportunity_id):
        self.session = session
        self.opportunity_id = opportunity_id

    def getDocuments(self):
        return map(
            lambda ld: ld.document,
            self.session.query(OpportunityDocument).filter(
                and_(
                    OpportunityDocument.opportunity_id == self.opportunity_id,
                    OpportunityDocument.folder_id == None
                )
            )
        )

    def getChildFolders(self):
        return map(
            lambda lf: OpportunityFolderController(self.session, lf),
            self.session.query(OpportunityFolder).filter(
                and_(
                    OpportunityFolder.opportunity_id == self.opportunity_id,
                    OpportunityFolder.parent_id == None
                )
            )
        )


class OpportunityFolderController(FolderController):
    # default constructor
    def __init__(self, session, opportunity_folder):
        self.session = session
        if not isinstance(opportunity_folder, OpportunityFolder):
            self.opportunity_folder = self.session.query(OpportunityFolder).filter(
                OpportunityFolder.id == opportunity_folder
            ).one()
        else:
            self.opportunity_folder = opportunity_folder

    def getName(self) -> String:
        return self.opportunity_folder.name

    def getParentName(self):
        if self.opportunity_folder.parent is not None:
            return self.opportunity_folder.parent.name
        else:
            None

    def getDocuments(self):
        return map(
            lambda ld: ld.document,
            self.session.query(OpportunityDocument).filter(OpportunityDocument.folder_id == self.opportunity_folder.id)
        )

    def getChildFolders(self):
        return map(
            lambda lf: OpportunityFolderController(self.session, lf),
            self.session.query(OpportunityFolder).filter(
                OpportunityFolder.parent_id == self.opportunity_folder.id
            )
        )


# contact controller
class RootContactFolderController(RootFolderController):
    def __init__(self, session, contact_id):
        self.session = session
        self.contact_id = contact_id

    def getDocuments(self):
        return map(
            lambda ld: ld.document,
            self.session.query(ContactDocument).filter(
                and_(
                    ContactDocument.contact_id == self.contact_id,
                    ContactDocument.folder_id == None
                )
            )
        )

    def getChildFolders(self):
        return map(
            lambda lf: ContactFolderController(self.session, lf),
            self.session.query(ContactFolder).filter(
                and_(
                    ContactFolder.contact_id == self.contact_id,
                    ContactFolder.parent_id == None
                )
            )
        )


class ContactFolderController(FolderController):
    # default constructor
    def __init__(self, session, contact_folder):
        self.session = session
        if not isinstance(contact_folder, ContactFolder):
            self.contact_folder = self.session.query(ContactFolder).filter(
                ContactFolder.id == contact_folder
            ).one()
        else:
            self.contact_folder = contact_folder

    def getName(self) -> String:
        return self.contact_folder.name

    def getParentName(self):
        if self.contact_folder.parent is not None:
            return self.contact_folder.parent.name
        else:
            None

    def getDocuments(self):
        return map(
            lambda ld: ld.document,
            self.session.query(ContactDocument).filter(ContactDocument.folder_id == self.contact_folder.id)
        )

    def getChildFolders(self):
        return map(
            lambda lf: ContactFolderController(self.session, lf),
            self.session.query(ContactFolder).filter(
                ContactFolder.parent_id == self.contact_folder.id
            )
        )


def lambda_handler(event, context):
    session = getSession()
    customer_id = event["customer_id"]

    errors = []
    types = ["loan", "opportunity", "contact"]
    sub_types = ["folder", "root"]

    if event["type"] in types:
        decorator_zip_name_type = event["type"].capitalize()
    else:
        errors.append("event[type] not in ({0})".format(", ".join(types)))

    if event["sub_type"] in sub_types:
        if event["sub_type"] == "folder":
            func_folder_controller = '{0}FolderController'
        elif event["sub_type"] == "root":
            func_folder_controller = 'Root{0}FolderController'
        func_folder_controller = func_folder_controller.format(decorator_zip_name_type)
    else:
        errors.append("event[sub_type] not in ({0})".format(", ".join(types)))

    folder = globals()[func_folder_controller](session, event['sub_type_id'])
    if not isinstance(folder, RootFolderController):
        decorator_zip_name_sub_type = "({0}) folder ({1})".format(
            getattr(
                getattr(
                    folder,
                    "{0}_folder".format(event["type"])
                ),
                "{0}_id".format(event["type"])
            ),
            folder.getName()
        )
    elif isinstance(folder, FolderController):
        decorator_zip_name_sub_type = "({0}) root folder".format(event["sub_type_id"])
    else:
        errors.append("Class error name ({0})".format(func_folder_controller))

    zip_name = "{0} {1} ({2}).zip".format(
        decorator_zip_name_type,
        decorator_zip_name_sub_type,
        datetime.now()
    )

    s3_client = boto3.client('s3')
    aws_bucket_src = "{0}".format(os.environ['aws_bucket_src'])
    aws_bucket_dst = "{0}".format(os.environ['aws_bucket_dst'])
    zipZipper(folder, s3_client, aws_bucket_src, aws_bucket_dst, customer_id, zip_name)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "zip_name": zip_name,
            "errors": "\n".join(errors)
        }),
    }
