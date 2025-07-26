"""
Pipeline lock mechanism to prevent concurrent pipeline runs.

This module provides functionality to create and manage pipeline locks
to ensure only one instance of the data loader can run at a time,
preventing concurrent modifications and ensuring idempotency.
"""

import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
from loguru import logger


class PipelineLockError(Exception):
    """Exception raised when pipeline lock operations fail."""
    pass


class PipelineLock:
    """
    Manages pipeline execution locks to prevent concurrent runs.
    
    Uses file-based locking with timeout and stale lock detection.
    """
    
    def __init__(self, lock_dir: str, lock_name: str = "pipeline", 
                 timeout_minutes: int = 120, check_interval_seconds: int = 5):
        """
        Initialize pipeline lock.
        
        Args:
            lock_dir: Directory to store lock files
            lock_name: Name of the lock (useful for multiple pipelines)
            timeout_minutes: Maximum time to hold a lock before considering it stale
            check_interval_seconds: How often to check for lock release when waiting
        """
        self.lock_dir = Path(lock_dir)
        self.lock_name = lock_name
        self.lock_file = self.lock_dir / f"{lock_name}.lock"
        self.timeout_minutes = timeout_minutes
        self.check_interval_seconds = check_interval_seconds
        
        # Ensure lock directory exists
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        
        # Track if we currently hold the lock
        self._acquired = False
        self._lock_data: Optional[Dict[str, Any]] = None
    
    def acquire(self, wait: bool = False, max_wait_minutes: int = 30) -> bool:
        """
        Acquire the pipeline lock.
        
        Args:
            wait: Whether to wait for lock if currently held
            max_wait_minutes: Maximum time to wait for lock acquisition
            
        Returns:
            True if lock acquired, False otherwise
            
        Raises:
            PipelineLockError: If lock acquisition fails
        """
        if self._acquired:
            logger.warning("Lock already acquired by this instance")
            return True
        
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        
        while True:
            try:
                # Check if lock exists and is valid
                if self.lock_file.exists():
                    existing_lock = self._read_lock_file()
                    
                    if existing_lock and not self._is_stale_lock(existing_lock):
                        if not wait:
                            logger.warning(f"Pipeline lock held by PID {existing_lock.get('pid')} since {existing_lock.get('acquired_at')}")
                            return False
                        
                        # Check if we've exceeded max wait time
                        if time.time() - start_time > max_wait_seconds:
                            raise PipelineLockError(f"Timeout waiting for lock after {max_wait_minutes} minutes")
                        
                        logger.info(f"Waiting for lock held by PID {existing_lock.get('pid')}...")
                        time.sleep(self.check_interval_seconds)
                        continue
                    else:
                        # Lock is stale, remove it
                        if existing_lock:
                            logger.warning(f"Removing stale lock from PID {existing_lock.get('pid')}")
                        self._remove_lock_file()
                
                # Try to acquire lock
                lock_data = {
                    "pid": os.getpid(),
                    "hostname": os.uname().nodename,
                    "acquired_at": datetime.utcnow().isoformat(),
                    "timeout_at": (datetime.utcnow() + timedelta(minutes=self.timeout_minutes)).isoformat(),
                    "lock_name": self.lock_name
                }
                
                # Atomic write with exclusive creation
                with open(self.lock_file, 'x') as f:
                    json.dump(lock_data, f, indent=2)
                
                self._acquired = True
                self._lock_data = lock_data
                logger.info(f"Pipeline lock acquired by PID {os.getpid()}")
                return True
                
            except FileExistsError:
                # Another process created the lock between our check and create
                if not wait:
                    return False
                    
                # Check wait timeout
                if time.time() - start_time > max_wait_seconds:
                    raise PipelineLockError(f"Timeout waiting for lock after {max_wait_minutes} minutes")
                
                time.sleep(self.check_interval_seconds)
                continue
                
            except Exception as e:
                raise PipelineLockError(f"Failed to acquire lock: {e}")
    
    def release(self) -> bool:
        """
        Release the pipeline lock.
        
        Returns:
            True if lock released successfully, False otherwise
        """
        if not self._acquired:
            logger.warning("No lock held by this instance")
            return True
        
        try:
            # Verify we still own the lock
            if self.lock_file.exists():
                current_lock = self._read_lock_file()
                if current_lock and current_lock.get('pid') == os.getpid():
                    self._remove_lock_file()
                    logger.info(f"Pipeline lock released by PID {os.getpid()}")
                else:
                    logger.warning("Lock file doesn't match current process")
            
            self._acquired = False
            self._lock_data = None
            return True
            
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
            return False
    
    def is_locked(self) -> bool:
        """
        Check if pipeline is currently locked by any process.
        
        Returns:
            True if locked, False otherwise
        """
        if not self.lock_file.exists():
            return False
        
        lock_data = self._read_lock_file()
        return lock_data is not None and not self._is_stale_lock(lock_data)
    
    def get_lock_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about current lock holder.
        
        Returns:
            Lock information dict or None if not locked
        """
        if not self.lock_file.exists():
            return None
        
        lock_data = self._read_lock_file()
        if lock_data and not self._is_stale_lock(lock_data):
            return lock_data
        
        return None
    
    def force_release(self) -> bool:
        """
        Force release the lock (use with caution).
        
        Returns:
            True if lock removed, False otherwise
        """
        try:
            if self.lock_file.exists():
                self._remove_lock_file()
                logger.warning("Pipeline lock force released")
            
            self._acquired = False
            self._lock_data = None
            return True
            
        except Exception as e:
            logger.error(f"Error force releasing lock: {e}")
            return False
    
    def _read_lock_file(self) -> Optional[Dict[str, Any]]:
        """Read and parse lock file content."""
        try:
            with open(self.lock_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error reading lock file: {e}")
            return None
    
    def _remove_lock_file(self):
        """Remove the lock file."""
        try:
            self.lock_file.unlink()
        except FileNotFoundError:
            pass  # Already removed
    
    def _is_stale_lock(self, lock_data: Dict[str, Any]) -> bool:
        """Check if a lock is stale (timeout expired or process no longer exists)."""
        try:
            # Check timeout
            timeout_str = lock_data.get('timeout_at')
            if timeout_str:
                timeout_dt = datetime.fromisoformat(timeout_str)
                if datetime.utcnow() > timeout_dt:
                    return True
            
            # Check if process still exists
            pid = lock_data.get('pid')
            if pid and isinstance(pid, int):
                try:
                    # Send signal 0 to check if process exists (doesn't actually send signal)
                    os.kill(pid, 0)
                    return False  # Process exists
                except OSError:
                    return True   # Process doesn't exist
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking stale lock: {e}")
            return True  # Assume stale if we can't verify
    
    def __enter__(self):
        """Context manager entry."""
        if not self.acquire(wait=False):
            raise PipelineLockError("Failed to acquire pipeline lock")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()
    
    def __del__(self):
        """Cleanup on destruction."""
        if self._acquired:
            self.release()