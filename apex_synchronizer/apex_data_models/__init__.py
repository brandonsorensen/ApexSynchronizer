"""
The :mod:`apex_data_models` package defines a number of objects that
aid in interfacing with the Apex API. Their job is is to facilitate
DELETE, GET, POST, and PUT operations and validate the responses
thereto.

The base :class:`ApexDataObject` class defines the interface and
implements a number of methods that do not require information specific
to an individual data object. It is abstract and as such cannot be
instantiated on its own.

Further, the module defines three subclasses to the
:class:`ApexDataObject`:

    - :class:`ApexStudent`
    - :class:`ApexStaffMember`
    - :class:`ApexClassroom`

Each of these classes is defined in their own submodules along with
various accompanying functions and type definitions. Further information
can be found in the docstrings of those modules.
"""

from .apex_data_object import ApexDataObject
from .apex_student import ApexStudent
from .apex_classroom import ApexClassroom, get_classrooms_for_eduids
from .apex_staff_member import ApexStaffMember
from .utils import BASE_URL, School, SCHOOL_CODE_MAP
