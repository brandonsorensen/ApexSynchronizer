"""
This module contains class definitions for the "delegate" classes for
use in the `apex_synchronizer` parent module. These delegates define
the sync behaviors of the various data objects in the Apex database.
There are four, mirroring the four data models found in the
`apex_data_models` "uncle" package:

`ClassroomDelegate` synchronizes PowerSchool sections with their
counterparts in Apex, referred to there as "classrooms".

`EnrollmentDelegate` manages classroom enrollment information.

`RosterDelegate` manages the student roster, adding and removing Apex
students based on the PowerSchool database.

`StaffDelegate` adds any PowerSchool staff member into Apex.

See the documentation for each class for more information about the
specifics of their routines.
"""
from .base_delegate import SyncDelegate
from .classroom_delegate import ClassroomDelegate
from .enrollment_delegate import EnrollmentDelegate
from .roster_delegate import RosterDelegate
from .staff_delegate import StaffDelegate
