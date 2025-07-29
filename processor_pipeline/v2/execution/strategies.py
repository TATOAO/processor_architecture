"""
Error handling strategies for the v2 processing pipeline.

Provides configurable error recovery strategies including skip, retry, and halt options.
"""

import asyncio
import time
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime, timedelta

from ..core.interfaces import ErrorHandlerInterface
from ..core.errors import ProcessorError, PipelineError, ErrorTracker


class ErrorStrategy(ErrorHandlerInterface):
    """Base class for error handling strategies"""
    
    def __init__(self, name: str, **kwargs):
        self.name = name
        self.config = kwargs
        self.error_count = 0
        self.last_error_time: Optional[datetime] = None
        self.logger = logging.getLogger(f"error_strategy_{name}")
        
    def get_strategy_name(self) -> str:
        return self.name
        
    async def handle_error(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        """Handle an error - subclasses should implement specific logic"""
        self.error_count += 1
        self.last_error_time = datetime.now()
        
        self.logger.error(f"Error in processor {processor_id}: {error}")
        return await self._handle_error_impl(error, processor_id, context)
        
    @abstractmethod
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        """Implement specific error handling logic"""
        pass
        
    def reset_error_count(self) -> None:
        """Reset error count - useful for retry strategies"""
        self.error_count = 0
        self.last_error_time = None


class SkipStrategy(ErrorStrategy):
    """Skip the current item and continue processing"""
    
    def __init__(self, max_skips: Optional[int] = None, skip_callback: Optional[Callable] = None):
        super().__init__("skip", max_skips=max_skips)
        self.max_skips = max_skips
        self.skip_callback = skip_callback
        self.skipped_items = []
        
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        # Check if we've exceeded max skips
        if self.max_skips is not None and self.error_count > self.max_skips:
            self.logger.error(f"Exceeded maximum skips ({self.max_skips}) for processor {processor_id}")
            return False
            
        # Log the skip
        skip_info = {
            "processor_id": processor_id,
            "error": str(error),
            "timestamp": datetime.now().isoformat(),
            "input_data": context.get("input_data")
        }
        self.skipped_items.append(skip_info)
        
        self.logger.warning(f"Skipping item in processor {processor_id} due to error: {error}")
        
        # Call skip callback if provided
        if self.skip_callback:
            try:
                await self.skip_callback(error, processor_id, context)
            except Exception as e:
                self.logger.error(f"Error in skip callback: {e}")
                
        return True  # Continue processing
        
    def get_skipped_items(self) -> List[Dict[str, Any]]:
        """Get list of skipped items"""
        return self.skipped_items.copy()
        
    def clear_skipped_items(self) -> None:
        """Clear the list of skipped items"""
        self.skipped_items.clear()


class RetryStrategy(ErrorStrategy):
    """Retry the operation with exponential backoff"""
    
    def __init__(self, max_retries: int = 3, initial_delay: float = 1.0, 
                 max_delay: float = 60.0, backoff_multiplier: float = 2.0,
                 retry_on: Optional[List[type]] = None):
        super().__init__("retry", max_retries=max_retries, initial_delay=initial_delay,
                        max_delay=max_delay, backoff_multiplier=backoff_multiplier)
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.retry_on = retry_on or [Exception]  # Retry on all exceptions by default
        
        # Track retry attempts per processor
        self.retry_counts: Dict[str, int] = {}
        self.retry_history: List[Dict[str, Any]] = []
        
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        # Check if we should retry this type of error
        if not any(isinstance(error, retry_type) for retry_type in self.retry_on):
            self.logger.info(f"Not retrying error type {type(error).__name__} for processor {processor_id}")
            return False
            
        # Get current retry count for this processor
        current_retries = self.retry_counts.get(processor_id, 0)
        
        if current_retries >= self.max_retries:
            self.logger.error(f"Exhausted retries ({self.max_retries}) for processor {processor_id}")
            return False
            
        # Calculate delay with exponential backoff
        delay = min(
            self.initial_delay * (self.backoff_multiplier ** current_retries),
            self.max_delay
        )
        
        self.logger.warning(f"Retrying processor {processor_id} (attempt {current_retries + 1}/{self.max_retries}) "
                           f"after {delay}s delay. Error: {error}")
        
        # Record retry attempt
        retry_info = {
            "processor_id": processor_id,
            "attempt": current_retries + 1,
            "max_retries": self.max_retries,
            "delay": delay,
            "error": str(error),
            "timestamp": datetime.now().isoformat()
        }
        self.retry_history.append(retry_info)
        
        # Update retry count
        self.retry_counts[processor_id] = current_retries + 1
        
        # Wait before retry
        await asyncio.sleep(delay)
        
        return True  # Continue processing (which will retry the operation)
        
    def reset_processor_retries(self, processor_id: str) -> None:
        """Reset retry count for a specific processor (call on success)"""
        self.retry_counts.pop(processor_id, None)
        
    def get_retry_history(self) -> List[Dict[str, Any]]:
        """Get retry history"""
        return self.retry_history.copy()


class HaltStrategy(ErrorStrategy):
    """Halt processing immediately on error"""
    
    def __init__(self, halt_callback: Optional[Callable] = None, 
                 cleanup_timeout: float = 30.0):
        super().__init__("halt", cleanup_timeout=cleanup_timeout)
        self.halt_callback = halt_callback
        self.cleanup_timeout = cleanup_timeout
        self.halt_reason: Optional[str] = None
        
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        self.halt_reason = f"Error in processor {processor_id}: {error}"
        
        self.logger.critical(f"Halting processing due to error in processor {processor_id}: {error}")
        
        # Call halt callback if provided
        if self.halt_callback:
            try:
                await asyncio.wait_for(
                    self.halt_callback(error, processor_id, context),
                    timeout=self.cleanup_timeout
                )
            except asyncio.TimeoutError:
                self.logger.error(f"Halt callback timed out after {self.cleanup_timeout}s")
            except Exception as e:
                self.logger.error(f"Error in halt callback: {e}")
                
        return False  # Stop processing
        
    def get_halt_reason(self) -> Optional[str]:
        """Get the reason for halting"""
        return self.halt_reason


class CircuitBreakerStrategy(ErrorStrategy):
    """Circuit breaker pattern - stop processing if error rate is too high"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0,
                 success_threshold: int = 3):
        super().__init__("circuit_breaker", failure_threshold=failure_threshold,
                        recovery_timeout=recovery_timeout, success_threshold=success_threshold)
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        # Circuit breaker state
        self.state = "closed"  # closed, open, half_open
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state_change_time = datetime.now()
        
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        self.success_count = 0  # Reset success count on failure
        
        if self.state == "closed":
            if self.failure_count >= self.failure_threshold:
                self._transition_to_open()
                return False
            return True  # Continue processing
            
        elif self.state == "half_open":
            # Failed during recovery - go back to open
            self._transition_to_open()
            return False
            
        else:  # state == "open"
            return False  # Circuit is open, don't process
            
    def record_success(self, processor_id: str) -> None:
        """Record a successful operation"""
        if self.state == "half_open":
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self._transition_to_closed()
        elif self.state == "closed":
            # Reset failure count on success
            self.failure_count = max(0, self.failure_count - 1)
            
    def _transition_to_open(self) -> None:
        """Transition to open state"""
        self.state = "open"
        self.state_change_time = datetime.now()
        self.logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
        
    def _transition_to_half_open(self) -> None:
        """Transition to half-open state"""
        self.state = "half_open"
        self.state_change_time = datetime.now()
        self.success_count = 0
        self.logger.info("Circuit breaker transitioning to half-open state")
        
    def _transition_to_closed(self) -> None:
        """Transition to closed state"""
        self.state = "closed"
        self.state_change_time = datetime.now()
        self.failure_count = 0
        self.success_count = 0
        self.logger.info("Circuit breaker closed - normal processing resumed")
        
    def should_attempt_processing(self) -> bool:
        """Check if processing should be attempted"""
        if self.state == "closed":
            return True
        elif self.state == "open":
            # Check if recovery timeout has elapsed
            if (self.last_failure_time and 
                datetime.now() - self.last_failure_time >= timedelta(seconds=self.recovery_timeout)):
                self._transition_to_half_open()
                return True
            return False
        else:  # half_open
            return True
            
    def get_circuit_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state"""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "failure_threshold": self.failure_threshold,
            "success_threshold": self.success_threshold,
            "state_change_time": self.state_change_time.isoformat(),
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
        }


class CompositeStrategy(ErrorStrategy):
    """Composite strategy that applies multiple strategies in sequence"""
    
    def __init__(self, strategies: List[ErrorStrategy], strategy_order: str = "sequential"):
        super().__init__("composite")
        self.strategies = strategies
        self.strategy_order = strategy_order  # "sequential" or "parallel"
        
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        if self.strategy_order == "sequential":
            return await self._handle_sequential(error, processor_id, context)
        elif self.strategy_order == "parallel":
            return await self._handle_parallel(error, processor_id, context)
        else:
            raise ValueError(f"Unknown strategy order: {self.strategy_order}")
            
    async def _handle_sequential(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        """Apply strategies in sequence until one succeeds"""
        for strategy in self.strategies:
            try:
                if await strategy.handle_error(error, processor_id, context):
                    return True
            except Exception as e:
                self.logger.error(f"Error in strategy {strategy.get_strategy_name()}: {e}")
                continue
        return False
        
    async def _handle_parallel(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        """Apply all strategies in parallel and combine results"""
        tasks = [
            asyncio.create_task(strategy.handle_error(error, processor_id, context))
            for strategy in self.strategies
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Return True if any strategy returned True
        return any(result is True for result in results if not isinstance(result, Exception))


class AdaptiveStrategy(ErrorStrategy):
    """Adaptive strategy that changes behavior based on error patterns"""
    
    def __init__(self, base_strategy: ErrorStrategy, adaptation_window: int = 100):
        super().__init__("adaptive")
        self.base_strategy = base_strategy
        self.adaptation_window = adaptation_window
        self.error_history: List[Dict[str, Any]] = []
        self.adaptation_rules: List[Dict[str, Any]] = []
        
    async def _handle_error_impl(self, error: Exception, processor_id: str, context: Dict[str, Any]) -> bool:
        # Record error in history
        error_record = {
            "error_type": type(error).__name__,
            "processor_id": processor_id,
            "timestamp": datetime.now(),
            "context": context
        }
        self.error_history.append(error_record)
        
        # Keep only recent errors
        if len(self.error_history) > self.adaptation_window:
            self.error_history = self.error_history[-self.adaptation_window:]
            
        # Analyze error patterns and adapt if needed
        await self._analyze_and_adapt()
        
        # Use current strategy
        return await self.base_strategy.handle_error(error, processor_id, context)
        
    async def _analyze_and_adapt(self) -> None:
        """Analyze error patterns and adapt strategy if needed"""
        if len(self.error_history) < 10:  # Need minimum data
            return
            
        # Analyze error patterns
        recent_errors = self.error_history[-10:]  # Last 10 errors
        error_types = [e["error_type"] for e in recent_errors]
        processors = [e["processor_id"] for e in recent_errors]
        
        # Example adaptation rules
        if len(set(error_types)) == 1 and len(recent_errors) >= 5:
            # Same error type repeatedly - might need different strategy
            dominant_error = error_types[0]
            self.logger.info(f"Detected repeated {dominant_error} errors, considering adaptation")
            
        if len(set(processors)) == 1 and len(recent_errors) >= 7:
            # Same processor failing repeatedly
            failing_processor = processors[0]
            self.logger.info(f"Processor {failing_processor} failing repeatedly, considering adaptation")
            
    def add_adaptation_rule(self, condition: Callable[[List[Dict[str, Any]]], bool],
                           new_strategy: ErrorStrategy) -> None:
        """Add a rule for strategy adaptation"""
        self.adaptation_rules.append({
            "condition": condition,
            "strategy": new_strategy
        })


def create_default_error_strategy() -> ErrorStrategy:
    """Create a sensible default error handling strategy"""
    return CompositeStrategy([
        RetryStrategy(max_retries=2, initial_delay=0.5),
        SkipStrategy(max_skips=10)
    ], strategy_order="sequential") 