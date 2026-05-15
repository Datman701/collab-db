"""Run all project tests."""
import sys
import os
import unittest

# Ensure project and bench-p01-crdt are importable.
root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root)
sys.path.insert(0, os.path.join(root, 'bench-p01-crdt'))

loader = unittest.TestLoader()
suite = unittest.TestSuite()

tests_dir = os.path.join(root, 'tests')
for fname in sorted(os.listdir(tests_dir)):
    if fname.startswith('test_task') and fname.endswith('.py'):
        mod_name = f"tests.{fname[:-3]}"
        try:
            mod = __import__(mod_name, fromlist=['*'])
            suite.addTests(loader.loadTestsFromModule(mod))
        except Exception as e:
            print(f"Failed to import {mod_name}: {e}")
            raise

runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

# Exit with non-zero on failure
sys.exit(0 if result.wasSuccessful() else 1)
