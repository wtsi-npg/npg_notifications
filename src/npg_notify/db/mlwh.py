# Copyright (C) 2024 Genome Research Ltd.
#
# Authors:
#   Marina Gourtovaia <mg8@sanger.ac.uk>
#   Kieron Taylor <kt19@sanger.ac.uk>
#
# This file is part of npg_notifications software package..
#
# npg_notifications is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Sftware Foundation; either version 3 of the License, or any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.

from sqlalchemy import ForeignKey, UniqueConstraint, Integer, String, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    Session,
)
from sqlalchemy.exc import NoResultFound

"""
Declarative ORM for some tables of multi-lims warehouse.
For simplicity, only columns used by this package are represented.
"""


class Base(DeclarativeBase):
    pass


class Study(Base):
    __tablename__ = "study"

    id_study_tmp = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_lims = mapped_column(String(10), nullable=False)
    id_study_lims = mapped_column(String(20), nullable=False)
    name = mapped_column(String(255))

    (
        UniqueConstraint(
            "id_lims",
            "id_study_lims",
            name="study_id_lims_id_study_lims_index",
        ),
    )

    study_users: Mapped[set["StudyUsers"]] = relationship()

    def __repr__(self):
        return f"Study {self.id_study_lims}, {self.name}"

    def contacts(self) -> list[str]:
        roles = ["manager", "follower", "owner"]
        return sorted(
            list(
                {
                    u.email
                    for u in self.study_users
                    if (u.email is not None and u.role is not None)
                    and (u.role in roles)
                }
            )
        )


class StudyUsers(Base):
    __tablename__ = "study_users"

    id_study_users_tmp = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    id_study_tmp = mapped_column(
        Integer, ForeignKey("study.id_study_tmp"), nullable=False, index=True
    )
    role = mapped_column(String(255), nullable=True)
    email = mapped_column(String(255), nullable=True)

    study: Mapped["Study"] = relationship(back_populates="study_users")

    def __repr__(self):
        role = self.role if self.role else "None"
        email = self.email if self.email else "None"
        return f"StudyUser role={role}, email={email}"


class StudyNotFoundError(Exception):
    pass


def get_study_contacts(session: Session, id: str):
    try:
        contacts = (
            session.execute(select(Study).where(Study.id_study_lims == id))
            .scalar_one()
            .contacts()
        )
    except NoResultFound:
        raise StudyNotFoundError(
            f"Study with ID {id} is not found in mlwarehouse"
        )

    return contacts
