package examples

type testDataset struct {
	items []string
}

func newSimpleTestDataset() testDataset {
	return testDataset{
		items: []string{
			"test1",
			"test2",
			"test3",
		},
	}
}

func (t testDataset) makeBytes() [][]byte {
	items := make([][]byte, len(t.items))
	for i, item := range t.items {
		items[i] = []byte(item)
	}
	return items
}

func (t testDataset) validate(output []string) bool {
	return isIn(t.items, output) && isIn(output, t.items)
}

func isIn(subSet, superSet []string) bool {
	for _, item1 := range subSet {
		found := false
		for _, item2 := range superSet {
			if item1 == item2 {
				found = true
			}
			break
		}
		if !found {
			return false
		}
	}
	return true
}
