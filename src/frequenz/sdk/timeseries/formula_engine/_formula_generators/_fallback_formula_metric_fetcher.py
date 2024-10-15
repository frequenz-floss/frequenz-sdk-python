# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""FallbackMetricFetcher implementation that uses formula generator."""

from frequenz.channels import Receiver

from ..._base_types import QuantityT, Sample
from .. import FormulaEngine
from .._formula_steps import FallbackMetricFetcher
from ._formula_generator import FormulaGenerator


# This is done as a separate module to avoid circular imports.
class FallbackFormulaMetricFetcher(FallbackMetricFetcher[QuantityT]):
    """A metric fetcher that uses a formula generator.

    The formula engine is generated lazily, meaning it is created only when
    the `start` or `fetch_next` method is called for the first time.
    Once the formula engine is initialized, it subscribes to its components
    and begins calculating and sending the formula results.
    """

    def __init__(self, formula_generator: FormulaGenerator[QuantityT]):
        """Create a `FallbackFormulaMetricFetcher` instance.

        Args:
            formula_generator: A formula generator that generates
                a formula engine with fallback components.
        """
        super().__init__()
        self._name = formula_generator.namespace
        self._formula_generator: FormulaGenerator[QuantityT] = formula_generator
        self._formula_engine: FormulaEngine[QuantityT] | None = None
        self._receiver: Receiver[Sample[QuantityT]] | None = None

    @property
    def name(self) -> str:
        """Get the name of the fetcher."""
        return self._name

    @property
    def is_running(self) -> bool:
        """Check whether the formula engine is running."""
        return self._receiver is not None

    def start(self) -> None:
        """Initialize the formula engine and start fetching samples."""
        engine = self._formula_generator.generate()
        # We need this assert because generate() can return a FormulaEngine
        # or FormulaEngine3Phase, but in this case we know it will return a
        # FormulaEngine. This helps to silence `mypy` and also to verify our
        # assumptions are still true at runtime
        assert isinstance(engine, FormulaEngine)
        self._formula_engine = engine
        self._receiver = self._formula_engine.new_receiver()

    async def ready(self) -> bool:
        """Wait until the receiver is ready with a message or an error.

        Once a call to `ready()` has finished, the message should be read with
        a call to `consume()` (`receive()` or iterated over).

        Returns:
            Whether the receiver is still active.
        """
        if self._receiver is None:
            self.start()

        assert self._receiver is not None
        return await self._receiver.ready()

    def consume(self) -> Sample[QuantityT]:
        """Return the latest message once `ready()` is complete."""
        assert (
            self._receiver is not None
        ), f"Fallback metric fetcher: {self.name} was not started"

        return self._receiver.consume()
