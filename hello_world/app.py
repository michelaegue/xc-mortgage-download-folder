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
from abc import abstractmethod
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


def getSession(event):
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
            event['db_user'],
            event['db_passwd'],
            event['db_server'],
            event['db_name']
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


class RootLoanFolderController(FolderController):
    def __init__(self, session, loan_id):
        self.session = session
        self.loan_id = loan_id

    def getName(self) -> String:
        return ""

    def getParentName(self):
        return None

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
            lambda lf: LoanFolder(self.session, lf),
            self.session.query(LoanFolder).filter(
                LoanFolder.parent_id == self.loan_folder.id
            )
        )


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    session = getSession(event)
    customer_id = event["customerId"]
    if "loan_folder_id" in event:
        folder = LoanFolderController(session, event["loan_folder_id"])
        if isinstance(folder, FolderController):
            zip_name = "{0}-{1}-{2}.zip".format(folder.getName(), event["loan_folder_id"], datetime.now())
    elif "loan_id" in event:
        folder = RootLoanFolderController(session, event["loan_id"])
        if isinstance(folder, FolderController):
            zip_name = "{0}-{1}.zip".format(event["loan_id"], datetime.now())
    s3_client = boto3.client('s3')
    aws_bucket_src = "{0}".format(event['aws_bucket_src'])
    aws_bucket_dst = "{0}".format(event['aws_bucket_dst'])
    zipZipper(folder, s3_client, aws_bucket_src, aws_bucket_dst, customer_id, zip_name)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": zip_name,
            # "location": ip.text.replace("\n", "")
        }),
    }
