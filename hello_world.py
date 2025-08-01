#!/usr/bin/env python3

def hello_world(name="World"):
    """Simple hello world function"""
    return f"Hello {name}!"

if __name__ == "__main__":
    print(hello_world())
    print(hello_world("Mitchell"))