0001 Purpose of This Repo
#########################

Status
******

Accepted

Context
*******

While developing the Open Analytics Reference System (OARS), the team found
a need to get certain information from the CMS into the analytics database.
This was found to be a common use case, and the design decisions around
edx-platform plugins and openedx-events made this kind of event sink an obvious
choice for moving data between systems.

Decision
********

We will create a repository to house an edx-platform plugin that can listen to
openedx-events and send them to ClickHouse, as well as working as priot art for
other future event sinks to different backends.

Consequences
************

A new repository will be created and maintained to house the code for this
project. ClickHouse will be able to receive event data from LMS and CMS. This
plugin will need to be installed to facilitate transfer of data in support of
OARS.

Rejected Alternatives
*********************

Originally the idea was to extend the existing `coursegraph`_ functionality
built into edx-platform and add a second backend for ClickHouse. This solved
the OARS use case, but locked out older named releases from using that
functionality and would have to be replicated for each individual type of
information we wanted to send in the future. It makes more sense to just have
one plugin that can be expanded to listen for more events.

.. _coursegraph: https://github.com/openedx/edx-platform/tree/master/cms/djangoapps/coursegraph
