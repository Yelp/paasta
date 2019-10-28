Simulation
==========

Running the Simulator
---------------------

.. program-output:: python -m clusterman.run simulate --help
   :cwd: ../../

.. _input_data_fmt:

Experimental Input Data
-----------------------

The simulator can accept experimental input data for one or more metric timeseries using the ``--metrics-data-file``
argument to ``clusterman simulate``.  The simulator expects this file to be stored as a compressed (gzipped) JSON file;
the JSON schema is as follows::

    {
        'metric_name_1': [
            [<date-time-string>, value],
            [<date-time-string>, value],
            ...
        ],
        'metric_name_2': [
            [<date-time-string>, value],
            [<date-time-string>, value],
            ...
        },
        ...
    }

.. _dict_data_fmt:

Optional Multi-valued Timeseries Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some timeseries data needs to have multiple y-values per timestamp.  The metrics data file can optionally accept
timeseries in a dictionary with the dictionary keys corresponding to the names of the individual timeseries.  For
example::

    {
        'metric_a': [
            [
                <date-time-string>,
                {
                  'key1': value,
                  'key2': value
                }
            ],
            [
                <date-time-string>,
                {
                  'key3': value
                }
            ],
            [
                <date-time-string>,
                {
                  'key1': value,
                  'key2': value,
                  'key3': value
                }
            ]
        ]
    }
