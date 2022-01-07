Dynamic Mapping & Collect
=========================

These APIs provide the means for a simple kind of *dynamic orchestration* — where the work to be orchestrated is determined not at pipeline definition time but at runtime, dependent on data that's observed as part of pipeline execution.

.. currentmodule:: dagster

.. autoclass:: DynamicOut

.. autoclass:: DynamicOutput


Mapping Details
---------------

To establish dependencies on a dynamic output in a composition function like :py:class:`job` or :py:class:`graph`, the ``map`` function is used on the object representing the dynamic output. `map` takes a ``Callable`` which receives a single argument. This callable is evaluated once, and the :py:class:`op` or :py:class:`graph` that are passed the input argument will establish dependencies. The return value is captured and wrapped in an object that allows for subsequent ``map`` or ``collect`` calls.

**Chaining**

The following two examples are equivalent ways to establish a sequence of :py:class:`op` that occur for each dynamic output.


.. code-block:: python

    @job
    def chained():
        results = dynamic_values().map(step_1).map(step_2).map(step_3)
        process(results.collect())


.. code-block:: python

    @job
    def chained():

        def _for_each(val):
            a = step_1(val)
            b = step_2(a)
            return step_3(b)

        results = dynamic_values().map(_for_each)

        process(results.collect())

**Additional Arguments**

A lambda or scoped function can be used to pass non-dynamic outputs along side dynamic ones in ``map`` downstream.

.. code-block:: python

    @job
    def other_arg():
        non_dynamic = plain_op()
        dynamic_values().map(lambda val: process(val, non_dynamic))

**Multiple Outputs**

Multiple outputs are returned via a ``namedtuple``

.. code-block:: python

    @job
    def multiple():
        # can unpack on assignment (order based)
        a_values, b_values = multiple_dynamic_values()
        a_values.map(process)
        do_stuff(b_values.collect())

This is true of downstream of dynamic outputs as well

.. code-block:: python

    @job
    def multiple_downstream():
        vals = dynamic_values()
        outs = vals.map(multi_out_op)

        # or unpack by name
        outs.output_a.map(process)
        do_stuff(outs.output_b.collect())
