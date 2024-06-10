# Getting started

## Prerequisites

1. Python 3.11 or newer installed on your system.
2. Access to a microgrid system supported by the `frequenz.sdk` or you can use
   the sandbox.
3. Basic knowledge of microgrid concepts.
4. [Familiarity with Channels](https://frequenz-floss.github.io/frequenz-channels-python/latest/).
5. [Install the Frequenz SDK](../index.md#installation)

## Create a project

### Create a Python file

You can start by simply creating a Python script (e.g., `pv_optimization.py`)
using your favorite text editor.

### Use Frequenz Repository Configuration

As an alternative and specially for larger projects, it's recommended to set up the
project using the [Frequenz Repository
Configuration](https://frequenz-floss.github.io/frequenz-repo-config-python/latest).

## Import necessary modules

You can now open the app's main file and start adding content. Begin by
importing the necessary libraries.

```python
import asyncio

from datetime import timedelta
from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig
```

## Create the application skeleton

The main logic of your application will run within an async function. Let's
create a skeleton that contains all the necessary code to initialize a
microgrid.

```python
async def run() -> None:
    # This points to the default Frequenz microgrid sandbox
    server_url = "grpc://microgrid.sandbox.api.frequenz.io:62060",

    # Initialize the microgrid
    await microgrid.initialize(
        server_url,
        ResamplerConfig(resampling_period=timedelta(seconds=1)),
    )

    # Define your application logic here
    # ...
```

## Define the `main()` function

Create a `main()` function that will set up and run the `run()` function using
asyncio.

```python
def main() -> None:
    asyncio.run(run())

if __name__ == "__main__":
    main()
```

## Implement the application logic

Inside the `run()` function, implement the core logic of your application. This
will include creating receivers for data streams, processing the data, making
decisions, and eventually sending control messages to the microgrid system. We
will cover more details in the following tutorials. For now, let's simply read
the power measurements from the microgrid's grid meter and print them on the
screen. The grid meter is a meter that is directly connected to the grid
connection point.

```python
async def run() -> None:
    # This points to the default Frequenz microgrid sandbox
    ...

    # Define your application logic here
    grid_meter = microgrid.grid().power.new_receiver()

    async for power in grid_meter:
        print(power.value)
```

## Putting it all together

Here is the full version of your first Frequenz SDK application.

```python
import asyncio

from datetime import timedelta
from frequenz.sdk import microgrid
from frequenz.sdk.actor import ResamplerConfig

async def run() -> None:
    # This points to the default Frequenz microgrid sandbox
    server_url = "grpc://microgrid.sandbox.api.frequenz.io:62060",

    # Initialize the microgrid
    await microgrid.initialize(
        server_url,
        ResamplerConfig(resampling_period=timedelta(seconds=1)),
    )

    # Define your application logic here
    grid_meter = microgrid.grid().power.new_receiver()

    async for power in grid_meter:
        print(power.value)

def main() -> None:
    asyncio.run(run())

if __name__ == "__main__":
    main()
```

## Run your application

You're now ready to run your application. When working on an existing
microgrid, make sure to update the `microgrid_host` and `microgrid_port`
variables before running the script.

```bash
# Example usage
python pv_optimization.py
```
