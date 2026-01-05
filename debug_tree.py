
import sys
import os
import json

sys.path.append(os.getcwd())

try:
    from server import _get_tree_logic

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

    def convert_to_visual_nodes(tree_data):
        """Convert API tree format to a generic list of nodes for printing"""
        root_children = []
        
        for co_name, co_data in tree_data.items():
            co_node = {'label': f"🏢 {co_name}", 'children': []}
            
            # Dept/Team types
            dept_team_targets = co_data.get('target_dept_or_team', {})
            
            for dtype, targets in dept_team_targets.items():
                type_icon = "👥" if dtype == 'team' else "department"
                # Simplify: skip the 'dept'/'team' grouping node if desired, 
                # or keep it. Let's list targets directly under Company for cleaner view,
                # maybe prefixed with [Dept] or [Team]
                
                for t_name, t_data in targets.items():
                    target_label = f"🎯 [{dtype.upper()}] {t_name}"
                    t_node = {'label': target_label, 'children': []}
                    
                    goals = t_data.get('goals', {})
                    for g_id, g_data in goals.items():
                        g_node = {'label': f"📝 {g_data['name']}", 'children': []}
                        
                        krs = g_data.get('krs', {})
                        for k_id, k_data in krs.items():
                            val = k_data.get('value', 0)
                            top = k_data.get('top_value', 0)
                            unit = k_data.get('unit', '')
                            kr_label = f"🔹 {k_data['name']} ({val}/{top} {unit})"
                            g_node['children'].append({'label': kr_label})
                        
                        t_node['children'].append(g_node)
                    
                    co_node['children'].append(t_node)
            
            root_children.append(co_node)
            
        return {'label': 'ROOT', 'children': root_children}

    print("Fetching OKR Tree...")
    tree_data = _get_tree_logic(ctx=MockCtx())
    
    print("\n" + "="*50)
    print("🌳 OKR TREE VISUALIZATION")
    print("="*50 + "\n")
    
    visual_root = convert_to_visual_nodes(tree_data)
    
    # We skip printing the ROOT node itself, just its children (Companies)
    for i, child in enumerate(visual_root['children']):
        print_tree(child, "", i == len(visual_root['children']) - 1)

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
