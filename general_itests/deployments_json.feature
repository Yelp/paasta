Feature: Per-Service Deployments.json can be written and read back
    Scenario: New services have a desired_state of "start"
	Given a test git repo is setup with commits
	 When paasta mark-for-deployments is run against the repo
	  And we generate deployments.json for that service
	Then that deployments.json has a desired_state of "start"

    Scenario: mark-for-deployments
	Given a test git repo is setup with commits
	 When paasta mark-for-deployments is run against the repo
	 Then the repository should be correctly tagged
