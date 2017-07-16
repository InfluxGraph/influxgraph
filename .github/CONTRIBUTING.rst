.. _contributing:

==============
 Contributing
==============

Thank you for considering to contribute to InfluxGraph. Any and all contributions are encouragaged and most welcome.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

There are many ways to contribute, from writing tutorials or blog posts, improving the documentation, contributing docker images for a particular task, submitting bug reports or feature requests or writing code to be incorporated into the project.

Please do not use the issue tracker for support questions. Use the `mail group`_ for that or other question/answer channels like Stack Overflow.

.. contents::
    :local:

Ground Rules
============

Please keep in mind the `Code of Conduct <https://github.com/InfluxGraph/influxgraph/blob/master/.github/code_of_conduct.md>`_ 
when making contributions.

Responsibilities

* Write `PEP-8 compliant <https://www.python.org/dev/peps/pep-0008/>`_ code of no more than 80 columns per line.
* Write appropriate unit and/or integration tests for new features or changes in functionality - see existing tests for reference. Tests that *mock* external services in lieu of writing integration tests will not be accepted.
* Changes should not adversely affect performance or scalability.
* Changes should not reduce code test coverage - this is a required check when merging pull requests.
* Be welcoming to newcomers and encourage diverse new contributors from all backgrounds. See the `Python Community Code of Conduct <https://www.python.org/psf/codeofconduct/>`_.

Unit or integration test?
--------------------------

The rule of thumb of unit test or integration test is:

* Is the code using an *external* service like InfluxDB? Integration test with that service.

* Is the code only used internally? Unit test.

In some cases, both unit and integration testing is needed to cover all usage of the functionality. For example and of particular importance to InfluxGraph, the Graphite metric path index functionality is one such case. It has both unit tests for its internal use and integration tests via its use in ``InfluxDBFinder``.

First Contributions
====================

Want to contribute but not sure where to start? Documentation is always a good place to start, especially if you are not a developer or do not want to write any code (yet!) and as is common with open source projects, documentation is most often lacking.

Beyond that, have a look at open issues on the issue tracker, particularly ones labelled as `PR wanted <https://github.com/InfluxGraph/influxgraph/issues?q=is%3Aissue+is%3Aopen+label%3A%22PR+wanted%22>`_.

For help in making pull requests, please see http://makeapullrequest.com/ and http://www.firsttimersonly.com/.

When a pull request is ready to be merged, the source branch containing your changes may need to be updated - *rebased* - because of conflicts with other changes to the code. See `Forking and setting up the repository`_ for details on how to do that.

.. _reporting-bugs:

Reporting Bugs
==============

The best way to report an issue and to ensure a timely response is to use the
issue tracker.

1) **Create a GitHub account**.

You need to `create a GitHub account`_ to be able to create new issues
and participate in the discussion.

.. _`create a GitHub account`: https://github.com/signup/free

2) **Determine if your bug is really a bug**.

A bug should not be filed to ask questions. For that you can use
the `mail group`_.

3) **Make sure the bug has not already been reported**.

Search through the appropriate Issue tracker. If a similar bug was found,
check if there is new information that could be reported to help
the developers fix the bug.

4) **Check if using the latest version**.

A bug could be fixed by some other improvements and fixes - it might not have an
existing report in the bug tracker. Make sure the latest release is being used.

5) **Collect information about the bug**.

To have the best chance of having a bug fixed, we need to be able to easily
reproduce the conditions that caused it. Most of the time this information
will be from a Python traceback message, though some bugs might be in design,
spelling or other errors on the documentation or code.

A) If the error is from a Python traceback, include it in the bug report.

B) We also need to know what platform you're running (Windows, macOS, Linux,
   docker container, etc.), the version of your Python interpreter, and the 
   version of InfluxGraph at the time of the error.

C) If at all possible, steps to reproduce including sample InfluxDB data are 
   the best way to help fixing the issue as quickly as possible.

There is also an issue template to help with creating issues.


6) **Submit the bug**.

By default `GitHub`_ will email you to let you know when new comments have
been made on your bug. In the event you've turned this feature off, you
should check back on occasion to ensure you do not miss any questions a
developer trying to fix the bug might ask.

.. _`GitHub`: https://github.com

Versions
========

Version numbers consists of a major version, minor version and a release number.
InfluxGraph uses the versioning semantics described by SemVer: http://semver.org.

All releases are published at PyPI when a versioned tag is pushed to the
repository. All tags are version numbers, for example ``1.3.0``.

.. _git-branches:

Working on Features & Patches
==============================

Forking and setting up the repository
-------------------------------------

Please see GitHub's instructions on `Fork a Repo`_ for getting started.

When the repository is cloned enter the directory to set up easy access
to upstream changes:

::

    cd influxgraph
    git remote add upstream git://github.com/InfluxGraph/influxgraph.git
    git fetch upstream

If you need to pull in new changes from upstream you should
always use the ``--rebase`` option to ``git pull``:

::

    git pull --rebase upstream master

With this option, history is not cluttered with merging
commit notes. See `Rebasing merge commits in git`_.
If you want to learn more about rebasing see the `Rebase`_
section in the GitHub guides.

Note that merge commits are not accepted when merging pull requests to upstream - either rebase or `squash commits <https://help.github.com/articles/about-merge-methods-on-github/#squashing-your-merge-commits>`_.

If you need to work on a different branch than the one git calls ``master``, you can
fetch and checkout a remote branch like this::

    git checkout --track -b 3.0-devel origin/3.0-devel

.. _`Fork a Repo`: https://help.github.com/fork-a-repo/
.. _`Rebasing merge commits in git`:
    https://notes.envato.com/developers/rebasing-merge-commits-in-git/
.. _`Rebase`: https://help.github.com/rebase/

Virtual environments
---------------------

It is highly recommended that `virtual environments <http://docs.python-guide.org/en/latest/dev/virtualenvs/>`_ are used for development and testing. This avoids system wide installation of dependencies, which may conflict with system provided libraries and other applications.

.. code-block:: shell

   virtualenv dev_env
   source dev_env/bin/activate

Integration testing
--------------------

The project has a heavy emphasis on integration testing as it directly
interfaces with external services like InfluxDB and memcached. To be able
to run integration tests for those services, they will need to be running on the
host running tests.

Please follow installation procedures for InfluxDB and Memcached for your
distribution or have a look at `automated installation steps for InfluxDB
used by Travis-CI builds <https://github.com/InfluxGraph/influxgraph/blob/master/.travis.yml#L20-L22>`_.

Official Docker images `are also available for easilly running InfluxDB <https://hub.docker.com/r/influxdata/influxdb/>`_.

Running the unit test suite
---------------------------

If you are working on development, then you need to
install the development requirements first:

.. code-block:: shell

   pip install -U -r requirements_dev.txt

Test suite is run via ``nosetests``. Simply calling ``nosetests`` in
the Git repository's root directory will run all available tests.

To run an individual test suite, call nosetests on a particular test file.

.. code-block:: shell

   nosetests tests/test_influxdb_integration.py

For seeing all log output, call nosetests like so:

.. code-block:: shell
  
   nosetests --nologcapture

To have nosetests fall back to a PDB prompt on uncaught exceptions, call it
like so:

.. code-block:: shell

   nosetests --pdb

If using manually set break points, via ``ipdb.set_trace()`` for example,
call nosetests like so:

.. code-block:: shell

   nosetests -s

to be able to fall back to an IPDB prompt.

Running an individual test within a test suite is also possible, for example:

.. code-block:: shell

   nosetests tests/test_influxdb_templates_integration.py:InfluxGraphTemplatesIntegrationTestCase.test_templated_index_find

will run the single test ``test_templated_index_find`` in ``test_influxdb_templates_integration.py``.

Creating pull requests
----------------------

When your feature/bugfix is complete you may want to submit
a pull requests so that it can be reviewed by the maintainers.

Creating pull requests is easy, and also let you track the progress
of your contribution. Read the `Pull Requests`_ section in the GitHub
Guide to learn how this is done.

You can also attach pull requests to existing issues by referencing the issue
number in the commit message, for example::

  git commit -m "Fixed <some bug> - resolves #22"

will refer to the issue #22, adding a message to the issue referencing the
commit and the PR, and automatically resolve the issue when the PR is merged. 

See `Closing issues using keywords`_ for more details.

.. _`Pull Requests`: http://help.github.com/send-pull-requests/

.. _`Closing issues using keywords`: https://help.github.com/articles/closing-issues-using-keywords/

Calculating test coverage
~~~~~~~~~~~~~~~~~~~~~~~~~

Add the ``--with-coverage`` flag to nosetests and call ``coverage report``
after tests have been completed.

.. code-block:: shell

   nosetests --with-coverage --cover-package=influxgraph
   coverage report

``coverage report -m`` will also show which lines are missing test coverage.

Running the tests on all supported Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All supported Python versions are tested by Travis-CI via test targets. For 
Travis-CI to run tests on a forked repository Travis-CI integration will need
to be enabled on that repository.

Building the documentation
--------------------------

Documentation is based on Sphinx, which needs to be installed to build it.

.. code-block:: shell

   pip install sphinx
   (cd docs; rm -rf _build; make html)

After building succeeds the documentation is available at ``_build/html``.

.. _contributing-verify:

Verifying your contribution
---------------------------

Required packages are installed by ``requirements_dev.txt`` per instructions
at `Running the unit test suite`_.

To ensure all tests are passing before committing, run the following in the
repository's root directory:

.. code-block:: shell

   nosetests

To ensure the code is PEP-8 compliant:

.. code-block:: shell

   flake8 influxgraph

To ensure documentation builds correctly:

.. code-block:: shell

   pip install sphinx
   (cd doc; make html)

Generated documentation will be found in ``doc/_build/html`` in the repository's
root directory.

.. _coding-style:

Coding Style
============

You should probably be able to pick up the coding style
from surrounding code, but it is a good idea to be aware of the
following conventions.

* All Python code must follow the `PEP-8 <https://www.python.org/dev/peps/pep-0008/>`_ guidelines.

``flake8`` and ``pep8`` are utilities you can use to verify that your code
is following the conventions. 

``flake8`` is automatically run by the project's
Travis-CI based integration tests and is required for builds to pass.

* Docstrings must follow the ``257`` conventions, and use the following
  style.

    Do this:

    ::

        def method(self, arg):
            """Short description.

            More details.

            """

    or:

    ::

        def method(self, arg):
            """Short description."""


    but not this:

    ::

        def method(self, arg):
            """
            Short description.
            """

* Docstrings for *public* API endpoints should include Sphinx docstring directives
  for inclusion in the auto-generated Sphinx based documentation. For example::

    def method(self, arg):
        """Method for <..>

	:param arg: Argument for <..>
	:type arg: str
	:rtype: None
	"""

  See existing documentation strings for reference.

* Docstrings for internal functions - ones starting with ``_`` or ``__`` are 
  not required.

* Lines should not exceed 80 columns.

* Import order

    * Python standard library (`import xxx`)
    * Python standard library (`from xxx import`)
    * Third-party packages.
    * Other modules from the current package.

    Within these sections the imports should be sorted by module name.

    Example:

    ::

        import threading
        import time

        from collections import deque
        from Queue import Queue, Empty

        from .platforms import Pidfile
        from .five import zip_longest, items, range
        from .utils.time import maybe_timedelta

* Wild-card imports must not be used (`from xxx import *`).

Release Procedure
=================

* Create new tag
* Add release notes for tag via GitHub releases

Creating a new tag can be done via the Releases page automatically if one does not already exist.

Auto-versioning from Git tags and revision
-------------------------------------------

The version number is automatically calculated based on, in order of 
preference:

* Git tag
* Latest git tag plus git revision short hand since tag

In order to publish a new version, just create and push a new tag.

::

    $ git tag X.Y.Z
    $ git push --tags

Releasing
---------

New git tags are automatically published to PyPi via Travis-CI deploy
functionality, subject to all tests and checks passing.

Aside from code tests, this includes documentation generating correctly for publishing to 
Read The Docs, style checks via ``flake8`` et al.

Publishing to PyPi and Read The Docs is only possible with Travis-CI build 
jobs initiated by the InfluxGraph GitHub project - forks 
cannot deploy to PyPi or publish documentation to Read The Docs.

Documentation is published to Read The Docs from both tags and master branch.

.. _`mail group`: https://groups.google.com/forum/#!forum/influxgraph
