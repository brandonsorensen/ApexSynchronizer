from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import ClassVar, Dict, List, Optional

from .utils import APEX_DATETIME_FORMAT


class ClassStatus(Enum):
	IN_PROGRESS = 1
	COMPLETED = 2
	WITHDRAWN = 3


@dataclass
class KeyPair(object):
	"""
	Data structure for use in the `ApexGradeReport` class's
	`_translation_pairs` class variable. Represents a relationship
	between internal instance variable references and their counterparts
	in Apex's JSON objects.
	"""
	internal: str
	apex: str
	is_date: bool  # Whether the object represents a date


@dataclass
class ApexGradeReport(object):

	"""
	Represents a grade report for a specific student for a specific
	class. This model represents each JSON entry in a GET call to a
	given classroom's "reports" URL:

		"{BASE_URL}/classrooms/{CLASSROOM_ID}/reports/"

	This class is meant to facilitate grade passback from Apex to
	PowerSchool.
	"""

	# For mapping between the internal snake-case variable names of this
	# class to the keys given by the Apex JSON objects
	_translation_pairs: ClassVar[List[KeyPair]] = [
		KeyPair(*pair) for pair in [
			('import_classroom_id', 'ImportClassroomId', False),
			('import_user_id', 'ImportUserId', False),
			('start_date', 'StudentStartDate', True),
			('product_code', 'ProductCode', False),
			('last_activity', 'DateOfLastActivity', True),
			('grade_to_date', 'GradeToDate', False),
			('final_grade', 'FinalGrade', False),
			('work_quality', 'QualityOfWork', False)
		]
	]

	import_classroom_id: int
	import_user_id: str
	start_date: datetime
	last_activity: datetime
	product_code: str
	grade_to_date: float
	work_quality: float
	final_grade: Optional[float]

	def __post_init__(self):
		"""
		Converts dates to `datetime` objects in case they are given
		as strings
		"""
		for attr in [pair.internal for pair in self._translation_pairs
					 if pair.is_date]:
			date = self.__getattribute__(attr)
			if isinstance(date, str):
				as_dt = datetime.strptime(date, APEX_DATETIME_FORMAT)
				self.__setattr__(attr, as_dt)

		try:
			self.final_grade = float(self.final_grade)
		except (TypeError, ValueError):
			self.final_grade = None

	@property
	def status(self) -> ClassStatus:
		""" Whether the student has finished the class yet."""
		if self.final_grade is None:
			return ClassStatus.IN_PROGRESS
		else:
			return ClassStatus.COMPLETED

	@classmethod
	def from_apex_json(cls, json_obj: Dict,
					   import_classroom_id: int) -> 'ApexGradeReport':
		"""
		Creates an `ApexGradeReport` object from the JSON response
		(provided as a `dict` object) returned by a call to the Apex
		API.

		:param dict json_obj: Apex grade report JSON object from Apex
								API
		:return: equivalent validated `ApexGradeReport` object
		"""
		json_obj['ImportClassroomId'] = import_classroom_id
		kwargs = {}
		for pair in cls._translation_pairs:
			kwargs[pair.internal] = json_obj[pair.apex]
		return cls(**kwargs)

	def to_json(self) -> Dict:
		"""
		Returns the object as a `dict` with keys matching those returned
		by GET calls to Apex's grade report. See the documentation of
		the `get_grade_reports` in the `ApexClassroom` definition.
		"""
		out = {}
		for pair in self._translation_pairs:
			val = getattr(self, pair.internal)
			if pair.is_date and val is not None:
				val = datetime.strftime(val, APEX_DATETIME_FORMAT)
			out[pair.apex] = val

		return out
