
import sys
import os
import json

sys.path.append(os.getcwd())

try:
    from server import _get_tree_logic, _convert_to_visual_nodes
    
    # Mock context
    class MockCtx:
        def info(self, m): pass
        def error(self, m): print(f"ERROR: {m}")
        
    def print_tree(node, prefix="", is_last=True):
        """Recursive function to print tree structure"""
        # Determine the branch characters
        connector = "└── " if is_last else "├── "
        
        # Print the current node data
        print(f"{prefix}{connector}{node['label']}")
        
        # Prepare prefix for children
        child_prefix = prefix + ("    " if is_last else "│   ")
        
        children = node.get('children', [])
        count = len(children)
        
        for i, child in enumerate(children):
            print_tree(child, child_prefix, i == count - 1)

    print("Fetching OKR Tree...")
    tree_data = _get_tree_logic(ctx=MockCtx())
    
    print("\n" + "="*50)
    print("🌳 OKR TREE VISUALIZATION")
    print("="*50 + "\n")
    
    visual_root = _convert_to_visual_nodes(tree_data)
    
    # We skip printing the ROOT node itself, just its children (Companies)
    for i, child in enumerate(visual_root['children']):
        print_tree(child, "", i == len(visual_root['children']) - 1)

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
