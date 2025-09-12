``#!/usr/bin/env python3
"""
Simple runner for the condominium debugging test
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from test_condo_debugging import CondoDebuggingTest


async def main():
    print("🔍 Starting Condominium Debugging Analysis...")
    print("This will connect to your actual database and analyze condo data.")
    print()
    
    diagnostic = CondoDebuggingTest()
    await diagnostic.run_diagnosis()
    
    print("\n" + "=" * 50)
    print("🎯 NEXT STEPS:")
    print("1. Review the debug output above")
    print("2. Check if condos exist but are archived/inactive")
    print("3. Look for accommodation category variations") 
    print("4. Run city snapshot creation to see debug logs")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
