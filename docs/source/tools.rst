Additional Tools
================

There are two ways to get data for simulation, if you would like to use values that are different than the actual
values recorded in the metrics store. :ref:`generate-data` allows you to generate values as a function of pre-existing metrics
or from a random distribution, and :ref:`signalfx_scraper` allows you to query values from SignalFX.

generate-data
-------------

The ``clusterman generate-data`` command is a helper function for the clusterman simulator to generate "fake" data,
either as some function of pre-existing metric data or as drawn from a specified random distribution.  The command takes
as input an experimental design YAML file, and produces as output a compressed JSON file that can be directly used in a
simulation.

.. note:: If the output file already exists, new generated metrics will be appended to it; existing metrics in the
   output file that share the same name as generated metrics will be overwritten, pending user confirmation


Experimental Design File Specification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An experimental design file contains details for how to generate experimental metric data for use in a simulation.  The
specification for the experimental design is as follows:

.. code-block:: yaml

    metric_type:
        metric_name:
            start_time: <date-time string>
            end_time: <date-time string>
            frequency: <frequency specification>
            values: <values specification>
            dict_keys: (optional) <list of dictionary keys>

* The ``metric_type`` should be one of the :ref:`metric_types`. There should be one section containing all the
  applicable metric names for each type.

* Each ``metric_name`` is arbitrary; it should correspond to a metric value that ``clusterman simulate`` will use when
  performing its simulation.  Multiple metrics can be specified for a given experimental design by repeating the above
  block in the YAML file for each desired metric; note that if multiple metrics should follow the same data generation
  specification, `YAML anchors and references <https://en.wikipedia.org/wiki/YAML#Advanced_components>`_ can be used.

* The ``<date-time string>`` fields can be in a wide variety of different formats, both relative and exact.  In most cases
  dates and times should be specifed in `ISO-8601 format <https://en.wikipedia.org/wiki/ISO_8601>`_; for example,
  ``2017-08-03T18:08:44+00:00``.  However, in some cases it may be useful to specify relative times; these can be in
  human-readable format, for example ``one month ago`` or ``-12h``.

* The ``<frequency specification>`` can take one of three formats:

  - Historical data: To generate values from historical values, specify ``historical`` here and follow
    the specification for historical values below.
  - Random data: if values will be randomly generated, then the frequency can be in one of two formats:

      - Regular intervals: by providing an ``<date-time string>`` for the frequency specification, metric values will be
        generated periodically; for example, a frequency of ``1m`` will generate a new data point every minute.
      - Random intervals: to generate new metric event arrival times randomly, specify a ``<random generator>`` block for
        the frequency, as shown below::

            distribution: dist-function
            params:
                dist_param_a: param-value
                dist_param_b: param-value

        The ``dist-function`` should be the name of a function in the `Python random module
        <https://docs.python.org/3/library/random.html#>`_.  The ``params`` are the keyword arguments for the chosen
        function.  All parameter values relating to time should be defined in seconds; for example, if ``gauss`` is chosen
        for the distribution function, the units for the mean and standard deviation should be seconds.

.. note:: A common choice for the dist-function is expovariate, which creates an exponentially-distributed interarrival
   time, a.k.a, a `Poisson process <https://en.wikipedia.org/wiki/Poisson_point_process>`_.  This is a good baseline
   model for the arrival times of real-world data.

* Similarly, the ``<values specification>`` can take one of two formats:

  - Function of historical data: historical values can be linearly transformed by :math:`ax+b`. Specify the following block::

        aws_region: <AWS region to read historical data from>
        params:
            a: <value>
            b: <value>

  - Random values: for this mode, specify a ``<random generator>`` block as shown above for frequency.

* The ``dict_keys`` field takes a list of strings which are used to generate a single timeseries with (potentially)
  multiple data points per time value.  For example, given the following ``dict_keys`` configuration::

    metric_a:
        dict_keys:
            - key1
            - key2
            - key3

  the resulting generated data for ``metric_a`` might look something like the example in :ref:`dict_data_fmt` format.

Output Format
~~~~~~~~~~~~~

The ``generate-data`` command produces a compressed JSON containing the generated metric data.  The format for this file
is identical to the simulator's :ref:`input_data_fmt` format.


Sample Usage
~~~~~~~~~~~~

::

    drmorr ~ > clusterman generate-data --input design.yaml --ouput metrics.json.gz
    Random Seed: 12345678

    drmorr ~ > clusterman simulate --metrics-data-file metrics.json.gz \
    > --start-time "2017-08-01T08:00:00+00:00" --end-time "2017-08-01T08:10:00+00:00"

    === Event 0 -- 2017-08-01T08:00:00+00:00        [Simulation begins]
    === Event 2 -- 2017-08-01T08:00:00+00:00        [SpotPriceChangeEvent]
    === Event 28 -- 2017-08-01T08:00:00+00:00       [SpotPriceChangeEvent]
    === Event 21 -- 2017-08-01T08:00:00+00:00       [SpotPriceChangeEvent]
    === Event 22 -- 2017-08-01T08:02:50+00:00       [SpotPriceChangeEvent]
    === Event 3 -- 2017-08-01T08:05:14+00:00        [SpotPriceChangeEvent]
    === Event 23 -- 2017-08-01T08:06:04+00:00       [SpotPriceChangeEvent]
    === Event 0 -- 2017-08-01T08:00:00+00:00        [Simulation ends]


Sample Experimental Design File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../examples/design.yaml
    :language: yaml

The above design file, and a sample output file are located in ``docs/examples/design.yaml`` and
``docs/examples/metrics.json.gz``, respectively.

.. _signalfx_scraper:

SignalFX scraper
----------------
A tool for downloading data points from SignalFX and saving them in the compressed JSON format that the Clusterman simulator can use.
This is an alternative to generating data if the data you're interested in is in SignalFX, but it's not yet in Clusterman metrics.

.. note:: Only data from the last month is available from SignalFX.

The tool will interactively ask you the :ref:`metric type <metric_types>` to save each metric as.

.. program-output:: python -m clusterman.tools.signalfx_scraper --help
   :cwd: ../../

Sample usage::

    python -m clusterman.tools.signalfx_scraper --start-time 2017-12-03 --end-time 2017-12-10 \
      --src-metric-names 'seagull.fleet_miser.cluster_capacity_units' --dest-file capacity \
      --api-token <secret> --filter rollup:max region:uswest2-testopia cluster_name:releng
