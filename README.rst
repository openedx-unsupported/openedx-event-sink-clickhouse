Event Sink ClickHouse
#####################

|pypi-badge| |ci-badge| |codecov-badge| |doc-badge| |pyversions-badge|
|license-badge| |status-badge|

Purpose
*******

This project acts as a plugin to the `Edx Platform`_, listens for
configured `Open edX events`_, and sends them to a `ClickHouse`_ database for
analytics or other processing. This is being maintained as part of the Open
Analytics Reference System (`OARS`_) project.

OARS consumes the data sent to ClickHouse by this plugin as part of data
enrichment for reporting, or capturing data that otherwise does not fit in
xAPI.

Sinks
*****

Currently the only sink is in the CMS. It listens for the ``COURSE_PUBLISHED``
signal and serializes a subset of the published course blocks into one table
and the relationships between blocks into another table. With those we are
able to recreate the "graph" of the course and get relevant data, such as
block names, for reporting.

Commands
********

In addition to being an event listener, this package provides commands for
exporting the same data in bulk. This allows bootstrapping a new data platform
or backfilling lost or missing data. Currently the only command is the Django
command for the ``COURSE_PUBLISHED`` data:

``python manage.py cms dump_courses_to_clickhouse``

This command allows bulk export of all courses, or various limiting factors.
Please see the command help for details:

``python manage.py cms dump_courses_to_clickhouse -h``


.. _Open edX events: https://github.com/openedx/openedx-events
.. _Edx Platform: https://github.com/openedx/edx-platform
.. _ClickHouse: https://clickhouse.com
.. _OARS: https://docs.openedx.org/projects/openedx-oars/en/latest/index.html

Getting Started
***************

Developing
==========

One Time Setup
--------------
.. code-block::

  # Clone the repository
  git clone git@github.com:openedx/openedx-event-sink-clickhouse.git
  cd openedx-event-sink-clickhouse

  # Set up a virtualenv using virtualenvwrapper with the same name as the repo and activate it
  mkvirtualenv -p python3.8 openedx-event-sink-clickhouse


Every time you develop something in this repo
---------------------------------------------
.. code-block::

  # Activate the virtualenv
  workon openedx-event-sink-clickhouse

  # Grab the latest code
  git checkout main
  git pull

  # Install/update the dev requirements
  make requirements

  # Run the tests and quality checks (to verify the status before you make any changes)
  make validate

  # Make a new branch for your changes
  git checkout -b <your_github_username>/<short_description>

  # Using your favorite editor, edit the code to make your change.
  vim ...

  # Run your new tests
  pytest ./path/to/new/tests

  # Run all the tests and quality checks
  make validate

  # Commit all your changes
  git commit ...
  git push

  # Open a PR and ask for review.

Deploying
=========

The Open edX Event Sink Clickhouse component is a django plugin which doesn't
need independent deployment. Therefore, its setup is reasonably
straightforward. First, it needs to be added to your service
requirements, and then it will be installed alongside requirements
of the service.

This plugin will be deployed by default in an OARS Tutor environment. For other
deployments install the library or add it to private requirements of your
virtual environment ( ``requirements/private.txt`` ).

#. Run ``pip install openedx-event-sink-clickhouse``.

#. Run migrations:

- ``python manage.py lms migrate``

- ``python manage.py cms migrate``

#. Restart LMS service and celery workers of edx-platform.

Configuration
===============

Currently all events will be listened to by default (there is only one). So
the only necessary configuration is a ClickHouse connection:

.. code-block::

    EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG = {
        # URL to a running ClickHouse server's HTTP interface. ex: https://foo.openedx.org:8443/ or
        # http://foo.openedx.org:8123/ . Note that we only support the ClickHouse HTTP interface
        # to avoid pulling in more dependencies to the platform than necessary.
        "url": "http://clickhouse:8123",
        "username": "changeme",
        "password": "changeme",
        "database": "event_sink",
        "timeout_secs": 3,
    }

Getting Help
************

Documentation
=============

See `documentation on Read the Docs <https://openedx-event-sink-clickhouse.readthedocs.io/en/latest/>`_.

More Help
=========

If you're having trouble, we have discussion forums at
https://discuss.openedx.org where you can connect with others in the
community.

Our real-time conversations are on Slack. You can request a `Slack
invitation`_, then join our `community Slack workspace`_.

For anything non-trivial, the best path is to open an issue in this
repository with as many details about the issue you are facing as you
can provide.

https://github.com/openedx/openedx-event-sink-clickhouse/issues

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx.org/slack
.. _community Slack workspace: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

License
*******

The code in this repository is licensed under the AGPL 3.0 unless
otherwise noted.

Please see `LICENSE.txt <LICENSE.txt>`_ for details.

Contributing
************

Contributions are very welcome.
Please read `How To Contribute <https://openedx.org/r/how-to-contribute>`_ for details.

This project is currently accepting all types of contributions, bug fixes,
security fixes, maintenance work, or new features.  However, please make sure
to have a discussion about your new feature idea with the maintainers prior to
beginning development to maximize the chances of your change being accepted.
You can start a conversation by creating a new issue on this repo summarizing
your idea.

The Open edX Code of Conduct
****************************

All community members are expected to follow the `Open edX Code of Conduct`_.

.. _Open edX Code of Conduct: https://openedx.org/code-of-conduct/

People
******

The assigned maintainers for this component and other project details may be
found in `Backstage`_. Backstage pulls this data from the ``catalog-info.yaml``
file in this repo.

.. _Backstage: https://open-edx-backstage.herokuapp.com/catalog/default/component/openedx-event-sink-clickhouse

Reporting Security Issues
*************************

Please do not report security issues in public. Please email security@openedx.org.

.. |pypi-badge| image:: https://img.shields.io/pypi/v/openedx-event-sink-clickhouse.svg
    :target: https://pypi.python.org/pypi/openedx-event-sink-clickhouse/
    :alt: PyPI

.. |ci-badge| image:: https://github.com/openedx/openedx-event-sink-clickhouse/workflows/Python%20CI/badge.svg?branch=main
    :target: https://github.com/openedx/openedx-event-sink-clickhouse/actions
    :alt: CI

.. |codecov-badge| image:: https://codecov.io/github/openedx/openedx-event-sink-clickhouse/coverage.svg?branch=main
    :target: https://codecov.io/github/openedx/openedx-event-sink-clickhouse?branch=main
    :alt: Codecov

.. |doc-badge| image:: https://readthedocs.org/projects/openedx-event-sink-clickhouse/badge/?version=latest
    :target: https://openedx-event-sink-clickhouse.readthedocs.io/en/latest/
    :alt: Documentation

.. |pyversions-badge| image:: https://img.shields.io/pypi/pyversions/openedx-event-sink-clickhouse.svg
    :target: https://pypi.python.org/pypi/openedx-event-sink-clickhouse/
    :alt: Supported Python versions

.. |license-badge| image:: https://img.shields.io/github/license/openedx/openedx-event-sink-clickhouse.svg
    :target: https://github.com/openedx/openedx-event-sink-clickhouse/blob/main/LICENSE.txt
    :alt: License

.. TODO: Choose one of the statuses below and remove the other status-badge lines.
.. |status-badge| image:: https://img.shields.io/badge/Status-Experimental-yellow
.. .. |status-badge| image:: https://img.shields.io/badge/Status-Maintained-brightgreen
.. .. |status-badge| image:: https://img.shields.io/badge/Status-Deprecated-orange
.. .. |status-badge| image:: https://img.shields.io/badge/Status-Unsupported-red
