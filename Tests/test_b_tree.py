from b_tree import BTree, BTreeNode, borrow_from_prev, borrow_from_next, merge_nodes, get_successor, get_predecessor


class TestBTree:
    def test_empty_tree_search(self):
        tree = BTree(min_pow=3)
        assert tree.search(tree.root, 10) is None, "Поиск в пустом дереве должен возвращать None"


    def test_insert_single_element(self):
        tree = BTree(min_pow=3)
        tree.insert((10, "val10"))
        assert tree.search(tree.root, 10) == "val10"


    def test_insert_multiple_elements_no_split(self):
        tree = BTree(min_pow=3)
        keys = [(5, "v5"), (1, "v1"), (3, "v3"), (2, "v2")]
        for k in keys:
            tree.insert(k)

        for k, v in keys:
            assert tree.search(tree.root, k) == v

        res = []
        tree.print_tree(res)
        sorted_keys = [kv[0] for kv in res]
        assert sorted_keys == [1, 2, 3, 5]


    def test_insert_causes_split(self):
        tree = BTree(min_pow=3)
        for i in range(1, 10):
            tree.insert((i, f"val{i}"))

        for i in range(1, 10):
            assert tree.search(tree.root, i) == f"val{i}"

        assert not tree.root.is_leaf, "Корень не должен быть листом после сплита"


    def test_search_non_existent(self):
        tree = BTree(min_pow=3)
        for i in range(10, 20):
            tree.insert((i, f"val{i}"))

        assert tree.search(tree.root, 5) is None


    def test_delete_leaf_key(self):
        tree = BTree(min_pow=3)
        tree.insert((10, "val10"))
        tree.insert((20, "val20"))
        tree.delete(tree.root, (10, "val10"))
        assert tree.search(tree.root, 10) is None
        assert tree.search(tree.root, 20) == "val20"


    def test_delete_internal_key(self):
        tree = BTree(min_pow=3)
        for i in [10, 20, 5, 15]:
            tree.insert((i, f"val{i}"))

        tree.delete(tree.root, (10, "val10"))
        assert tree.search(tree.root, 10) is None
        assert tree.search(tree.root, 20) == "val20"


    def test_borrow_from_prev(self):
        parent = BTreeNode(is_leaf=False)
        left_child = BTreeNode(is_leaf=True)
        right_child = BTreeNode(is_leaf=True)

        parent.keys = [(10, "v10")]
        parent.children = [left_child, right_child]

        left_child.keys = [(5, "v5")]
        right_child.keys = [(15, "v15")]

        right_child.keys.clear()
        right_child.keys = []
        borrow_from_prev(parent, 1)
        assert right_child.keys == [(10, "v10")]
        assert parent.keys == [(5, "v5")]
        assert left_child.keys == []


    def test_borrow_from_next(self):
        parent = BTreeNode(is_leaf=False)
        left_child = BTreeNode(is_leaf=True)
        right_child = BTreeNode(is_leaf=True)

        parent.keys = [(10, "v10")]
        parent.children = [left_child, right_child]

        left_child.keys = []
        right_child.keys = [(15, "v15")]

        borrow_from_next(parent, 0)
        assert left_child.keys == [(10, "v10")]
        assert parent.keys == [(15, "v15")]
        assert right_child.keys == []


    def test_merge_nodes(self):
        parent = BTreeNode(is_leaf=False)
        left_child = BTreeNode(is_leaf=True)
        right_child = BTreeNode(is_leaf=True)

        parent.keys = [(10, "v10")]
        parent.children = [left_child, right_child]

        left_child.keys = [(5, "v5")]
        right_child.keys = [(15, "v15")]

        merge_nodes(parent, 0)
        assert parent.keys == []
        assert len(parent.children) == 1
        assert parent.children[0].keys == [(5, "v5"), (10, "v10"), (15, "v15")]


    def test_get_successor(self):
        root = BTreeNode(is_leaf=False)
        right_child = BTreeNode(is_leaf=True)
        root.children = [right_child]
        right_child.keys = [(20, "v20"), (30, "v30")]

        assert get_successor(root) == (20, "v20")


    def test_get_predecessor(self):
        root = BTreeNode(is_leaf=False)
        left_child = BTreeNode(is_leaf=True)
        root.children = [left_child]
        left_child.keys = [(5, "v5"), (10, "v10")]

        assert get_predecessor(root) == (10, "v10")
