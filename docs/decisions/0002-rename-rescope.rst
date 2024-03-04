0002 Rename and Re-scope This Repository
########################################

Status
******

Accepted

Context
*******

As development of the Aspects project (formerly OARS) has continued, this repository has become more tightly bound to Aspects itself. To our knowledge no other adoption of this repository has taken place. As such, the maintainers would like to make this explicit and widen the scope of the repository to include all edx-platform plugin functionality for Aspects.

Decisions
*********

* We will deprecate this repository and move the existing functionality to the platform-plugin-aspects repository
* We will deprecate the PyPI project openedx-event-sink-clickhouse in favor of a combined platform-plugin-aspects project
* We will ensure that all of the existing functionality will be moved, and can be turned on and off via configuration so any current use cases of this repository will not be forced to use other functionality of the new repository

Consequences
************

* We will need to update the PyPI project name and mark the old project as deprecated
* We will need to upgrade tutor-contrib-aspects to use the new PyPI project
* Anyone using the current project will need to change their usage to the new repository, and update their configuration

Rejected Alternatives
*********************

* Maintaining several different plugins, they are going to be too tightly bound to the Aspects release and should be versioned together
