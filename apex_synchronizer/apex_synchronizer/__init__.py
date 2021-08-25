"""
This module coordinates the synchronization of the Apex and PowerSchool
databases. The main class is `ApexSynchronizer`, found in the
`apex_synchronizer` submodule. This main class makes use of four
"delegate" classes, found in the `delegates` submodule. Each of these
classes subclasses the `SyncDelegate` interface and contain the logic
for synchronizing a specific kind of object:

    - `ClassroomDelegate` synchronizes PowerSchool sections with their
        counterparts in Apex, referred to there as "classrooms".
    - `EnrollmentDelegate` manages classroom enrollment information.
    - `RosterDelegate` manages the student roster, adding and removing
        Apex students based on the PowerSchool database.
    - `StaffDelegate` adds any PowerSchool staff member into Apex.
"""


from .apex_synchronizer import ApexSynchronizer