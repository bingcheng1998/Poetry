# coding: utf-8

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import (
    Column, DateTime, ForeignKey, Index, Integer, String,
    Text, create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine("mysql+mysqldb://root:111@127.0.0.1:3306/poetry?charset=utf8mb4", max_overflow=5)

session = sessionmaker(bind=engine)()
Base = declarative_base()


def init_db():
    Base.metadata.create_all(engine)


class Author(Base):
    __tablename__ = 'author'

    id = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False)
    dynasty = Column(String(10), nullable=False)
    description = Column(Text, nullable=True)
    created = Column(DateTime, nullable=False, default=datetime.now)
    updated = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index('ix_name', 'name'),
        Index('ix_dynasty', 'dynasty'),
    )


class Poetry(Base):
    __tablename__ = 'poetry'

    id = Column(Integer, primary_key=True)
    author = Column(Integer, ForeignKey("author.id"), doc='作者')
    title = Column(String(200), nullable=False, default='', doc='标题')
    content = Column(Text, nullable=False, doc='内容')
    dynasty = Column(String(10), nullable=False, doc='朝代')
    created = Column(DateTime, nullable=False, default=datetime.now)
    updated = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index('ix_author', 'author'),
        Index('ix_title', 'title'),
        Index('ix_dynasty', 'dynasty'),
    )


def read_file(file_name):
    return pd.read_csv(file_name).dropna(how='any')


def update_db(df):
    for _, row in df.iterrows():
        title = row['题目']
        dynasty = row['朝代']
        author = row['作者']
        content = row['内容']
        db_author = session.query(Author).filter_by(name=author, dynasty=dynasty).first()
        if not db_author:
            author = Author(
                name=author,
                dynasty=dynasty,
                description=''
            )
            session.add(author)
            session.commit()
        else:
            author = db_author
        if not session.query(Poetry).filter_by(content=content, title=title).first():
            poetry = Poetry(
                title=title,
                author=author.id,
                content=content,
                dynasty=dynasty,
            )
            session.add(poetry)
            session.commit()


def get_files():
    files = os.listdir(os.curdir)
    csv_files = []
    for f in files:
        if f.endswith('csv'):
            csv_files.append(f)
    csv_files.sort()
    return csv_files


def insert_db():
    files = get_files()
    for f in files:
        update_db(read_file(f))


if __name__ == '__main__':
    init_db()
    insert_db()
