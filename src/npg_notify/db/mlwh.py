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

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)

"""
Declarative ORM for some tables of multi-lims warehouse (mlwh) database.
For simplicity, only columns used by this package are represented.

Available ORM classes: Study, StudyUser.

Utility methods: get_study_contacts.
"""

"Study contacts with these roles will receive notifications."
ROLES = ["manager", "follower", "owner"]


class Base(DeclarativeBase):
    pass


class Study(Base):
    "A representation for the 'study' table."

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

    study_users: Mapped[set["StudyUser"]] = relationship()

    def __repr__(self):
        return f"Study {self.id_study_lims}, {self.name}"

    def contacts(self) -> list[str]:
        """Retrieves emails of contacts for this study object.

        Returns:
          A sorted list of unique emails for managers, followers or owners of
          the study.
        """

        # In order to eliminate repetition, the comprehension expression below
        # returns a set, which is then sorted to return a sorted list.
        return sorted(
            {
                u.email
                for u in self.study_users
                if (u.email is not None and u.role is not None)
                and (u.role in ROLES)
            }
        )


class StudyUser(Base):
    "A representation for the 'study_users' table."

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
    "An error to use when a study does not exist in mlwh."

    pass


def get_study_contacts(session: Session, id: str) -> list[str]:
    """Retrieves emails of study contacts from the mlwh database.

    Args:
      session:
        sqlalchemy.orm.Session object
      id:
        String study ID.

    Returns:
      A sorted list of unique emails for managers, followers or owners of
      the study.

    Example:

      from npg_notify.db.mlwh get_study_contacts
      contact_emails = get_study_contacts(session=session, id="5901")
    """
    try:
        contacts = (
            session.execute(select(Study).where(Study.id_study_lims == id))
            .scalar_one()
            .contacts()
        )
    except NoResultFound:
        raise StudyNotFoundError(
            f"Study with ID {id} is not found in ml warehouse"
        )

    return contacts
