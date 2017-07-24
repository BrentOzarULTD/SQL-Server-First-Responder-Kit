# Contributing to the SQL Server First Responder Kit

First of all, welcome! We're excited that you'd like to contribute. How would you like to help?

* [I'd like to report a bug or request an enhancement](#how-to-report-bugs-or-request-enhancements)
* [I'd like to write new T-SQL checks](#how-to-write-new-t-sql-checks)
* [I'd like to fix bugs in T-SQL checks](#how-to-fix-bugs-in-existing-t-sql-checks)
* [I'd like to test checks written by someone else](#how-to-test-checks-written-by-someone-else)
* [I'd like to write or update documentation](#how-to-write-or-update-documentation)
* [I don't know how to upload code to GitHub](https://www.brentozar.com/archive/2015/07/pull-request-101-for-dbas-using-github/)

Everyone here is expected to abide by the [Contributor Covenant Code of Conduct](#the-contributor-covenant-code-of-conduct).

Wanna do something else, or have a question not answered here? Hop into Slack and ask us questions before you get started. [Get an invite to SQLCommunity.slack.com](https://sqlps.io/slack/), and we're in the [#FirstResponderKit channel](https://sqlcommunity.slack.com/messages/firstresponderkit/). We welcome newcomers, and there's always a way you can help.

## How to Report Bugs or Request Enhancements

Check out the [Github issues list]. Search for what you're interested in - there may already be an issue for it. Make sure to search through closed issues, too, because we often decline things that aren't a good fit for these tools.

If you can't find a similar issue, go ahead and open your own. Include as much detail as you can - what you're seeing now, and what you'd like to see.

When requesting new checks, keep in mind that we want to focus on:

* Actionable warnings - SQL Server folks are usually overwhelmed with data, and we only want to report on things they can actually do something about
* Performance issues or reliability risks - if it's just a setting we don't agree with, let's set that aside
* Things that end users or managers will notice - if we're going to have someone change a setting on their system, we want it to be worth their time

Now head on over to the [Github issues list] and get started.

## How to Write New T-SQL Checks

Before you code, check the [Github issues list] for what you're trying to do - there may already be an issue for it. Make sure to search through closed issues, too, because we often decline things that aren't a good fit for these tools.

If you've got a new idea that isn't covered in an existing issue, open a Github issue for it. Outline what you'd like to do, and how you'd like to code it. This just helps make sure other users agree that it's a good idea to add to these tools.

After a discussion, to start coding, [open a new Github branch.](https://www.brentozar.com/archive/2015/07/pull-request-101-for-dbas-using-github/) This lets you code in your own area without impacting anyone else. When your code is ready, test it on a case-sensitive instance of the oldest supported version of SQL Server (2008), and the newest version (2016).

When it's ready for review, make a pull request, and one of the core contributors can check your work.

## How to Fix Bugs in Existing T-SQL Checks

(stub)

## How to Test Checks Written by Someone Else

(stub)

* Test only on case-sensitive instances. A surprising number of folks out there run on these.
* Test on as many currently-supported versions of SQL Server as possible. At minimum, test on the oldest version (currently 2008), and the newest version (currently 2016).

## How to Write or Update Documentation

(stub)

## The Contributor Covenant Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as contributors and maintainers pledge to making participation in our project and our community a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, gender identity and expression, level of experience, nationality, personal appearance, race, religion, or sexual identity and orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

* Using welcoming and inclusive language
* Being respectful of differing viewpoints and experiences
* Gracefully accepting constructive criticism
* Focusing on what is best for the community
* Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery and unwelcome sexual attention or
  advances
* Trolling, insulting/derogatory comments, and personal or political attacks
* Public or private harassment
* Publishing others' private information, such as a physical or electronic
  address, without explicit permission
* Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable behavior and are expected to take appropriate and fair corrective action in response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or reject comments, commits, code, wiki edits, issues, and other contributions that are not aligned to this Code of Conduct, or to ban temporarily or permanently any contributor for other behaviors that they deem inappropriate, threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces when an individual is representing the project or its community. Examples of representing a project or community include using an official project e-mail address, posting via an official social media account, or acting as an appointed representative at an online or offline event. Representation of a project may be further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by contacting the project team at Help@BrentOzar.com. All complaints will be reviewed and investigated and will result in a response that is deemed necessary and appropriate to the circumstances. The project team is obligated to maintain confidentiality with regard to the reporter of an incident. Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good faith may face temporary or permanent repercussions as determined by other members of the project's leadership.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage], version 1.4,
available at [http://contributor-covenant.org/version/1/4][version]


## Git Flow for pull requests
<a name="git-flow"></a>

1. [Fork] the project, clone your fork, and configure the remotes:

   ```bash
   # Clone your fork of the repo into the current directory
   git clone git@github.com:<YOUR_USERNAME>/SQL-Server-First-Responder-Kit.git
   # Navigate to the newly cloned directory
   cd SQL-Server-First-Responder-Kit
   # Assign the original repo to a remote called "upstream"
   git remote add upstream https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
   ```

2. If you cloned a while ago, get the latest changes from upstream:

   ```bash
   git checkout master
   git pull upstream master
   ```

3. Create a new topic branch (off the main project development branch) to
   contain your feature, change, or fix:

   ```bash
   git checkout -b <topic-branch-name>
   ```

4. Commit your changes in logical chunks. Please adhere to these [git commit message guidelines]
   or your code is unlikely be merged into the main project. Use Git's [interactive rebase]
   feature to tidy up your commits before making them public.

5. Locally merge (or rebase) the upstream development branch into your topic branch:

   ```bash
   git pull [--rebase] upstream master
   ```

6. Push your topic branch up to your fork:

   ```bash
   git push origin <topic-branch-name>
   ```

7. [Open a Pull Request] with a clear title and description.

**IMPORTANT**: By submitting a patch, you agree to allow the project owner to license your work under the MIT [LICENSE]


[homepage]: http://contributor-covenant.org
[version]: http://contributor-covenant.org/version/1/4/
[Github issues list]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues
[Fork]:https://help.github.com/articles/fork-a-repo/
[git commit message guidelines]:http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
[interactive rebase]:https://help.github.com/articles/about-git-rebase/
[Open a Pull Request]:https://help.github.com/articles/about-pull-requests/
[LICENSE]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/master/LICENSE.md
