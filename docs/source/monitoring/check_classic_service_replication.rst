check_classic_service_replication
=================================

``check_classic_service_replication`` is a tool used to automatically monitor classic Yelp
soa services that are in Smartstack. Users can configure this tool via ``monitoring.yaml``
in yelpsoa-configs.

Monitoring.yaml
===============

The ``soa-configs`` house a file called monitoring.yaml whose keys are exposed through service_configuration_lib. You can use it to specify information relevant to monitoring for your service, such as who is responsible for the service, who to contact when it has problems, and even what to look for to ensure it stays up.

Available Keys
--------------
Within your monitoring.yaml file you can specify the following keys:
* team (mandatory): The team that owns the service. If this is not defined then no alerts will ever fire

  * This team key must be already defined in puppet for sensu to recognize it. This is a one time thing, once the team-data exists, your team can use sensu.

* page: A boolean that indicates if this service's alerts should be email only or escalate to a pager

  * Set this to true ONLY if you would want to be woken up in the middle of the night to fix the problem. If you wouldn't, leave page set to false, let it email (or file a ticket or something), and deal with it in the morning

* service_type (mandatory if you want automatic monitoring): The type of service the service is, can be 'classic' or 'marathon'

  * See `Automatic Replication Monitoring`_

* runbook: A link to the service's runbook, usually y/ links are appreciated as opposed to gdocs.

* tip: A tip for any oncall members responding to a page about this service. It is nice to put some info about who to talk to or how important this service is (e.g. if user is seeing 500s you might want to mention that, vs a service that gracefully degrades).

* alert_after: A human readable string (e.g. 0s, 1m, 12h) that is how long to wait before notifying oncall members of this service's failure

* realert_every: How many intervals of the alert_after time (e.g. -1, 1, 2, 3) to wait before re alerting oncall, you probably only care about this for email alerts

* notification_email: Usually you should rely on the email address associated with your `team` key, but if you need to override that email you can with this key

  * From kwa: "This becomes more obvious when a team has 10 services. They don't need to duplicate their email in 10 places. Common.yaml is where the team defaults live."

Automatic Replication Monitoring
--------------------------------
There are some additional sections you can add to get some cool monitoring for free. Currently this is limited to replication monitoring.

**WARNING: For this to work your team MUST be defined in Sensu.**

**WARNING2: This replication monitoring ONLY works on Smartstack-enabled services.**


Replication
^^^^^^^^^^^
If you add a key called 'replication' that has the following information under it, then your service will be automatically monitored for replication in the specified environments. More information and context is supplied in `this google doc <https://docs.google.com/a/yelp.com/document/d/1yPHXCe4LirTcStY7jLYPfFKjmuxvGzPW0BRK1O1iEYM/edit>`_. **This check only really makes sense if you are using smartstack** to route requests since the check will be looking at a smartstack haproxy instance.

* key: The file in /nail/etc to use to get the key to lookup in the map (e.g. 'habitat' gets the value of the key out of /nail/etc/habitat)
* default: The minimum number of instances to expect if you don't have a key that matches into the map
* map: A key, value map that maps environments to the _minimum_ number of instances you expect in that environment.

Examples
--------

Federator
^^^^^^^^^

A well formed monitoring.yaml::

    team: search_infra
    notification_email: search@yelp.com
    page: true
    runbook: 'y/rb-federator'
    tip: 'The federator service is in the critical path for search, you should be fixing this'
    
    service_type: 'classic'
    replication:
        key: 'region'
        default: 0
        map:
            region1: 2
            region2: 3
            region3: 1

This config would lead to pages if the federator has less than 2 instances in region1, 3 in region2, or 1 in region3. The default of 0 says that we should not page on under-replication in other environments (such as stage or dev). If you reload your service then you may want to consider a default of 1 because reloads should never kill your last instance in the load balancer.

Code modules
############

.. automodule:: paasta_tools.monitoring.check_classic_service_replication
   :members:


