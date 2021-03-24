soa-configs
===========

soa-configs are the shared configuration storage that PaaSTA uses to hold the
description and configuration of what services exist and how they should be
deployed and monitored.

This directory needs to be deployed globally in the same location to every
server that runs any PaaSTA component.

PaaSTA reads particular config files for each service in the soa-configs
directory. There is one folder per service. Here is an example tree::

  soa-configs
  ├── web
  │   ├── deploy.yaml
  │   ├── kubernetes-dev.yaml
  │   ├── kubernetes-prod.yaml
  │   ├── monitoring.yaml
  │   ├── service.yaml
  │   └── smartstack.yaml
  ├── api
  │   ├── adhoc-prod.yaml
  │   ├── deploy.yaml
  │   ├── marathon-dev.yaml
  │   ├── marathon-prod.yaml
  │   ├── monitoring.yaml
  │   ├── service.yaml
  │   ├── smartstack.yaml
  │   └── tron-prod.yaml
  ...

See the `paasta-specific soa-configs documentation <yelpsoa_configs.html>`_ for more information
about the structure and contents of some example files in soa-configs that PaaSTA uses.

For more information about why we chose this method of config distribution,
watch `this talk on Yelp's soa-configs and how it is used <https://vimeo.com/141231345>`_.

For reading soa-configs, PaaSTA uses `service_configuration_lib <https://github.com/Yelp/service_configuration_lib>`_.
