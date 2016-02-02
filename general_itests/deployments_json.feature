Feature: Per-Service Deployments.json can be written and read back

    Scenario: Per-Service Deployments.json can be written and read back
       Given a test git repo is setup with commits
	When paasta stop is run against the repo
	 And we generate deployments.json for that service
        Then that deployments.json can be read back correctly

    Scenario: mark-for-deployments
	Given a test git repo is setup with commits
	 When paasta mark-for-deployments is run against the repo
	 Then the repository should be correctly tagged
