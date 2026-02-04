package skiplist

import "bytes"

func compareElement(a *SkipListElement, key []byte) int {
	if len(a.GetKey()) == 0 {
		return -1
	}

	return bytes.Compare(a.GetKey(), key)
}

func (node *SkipListElement) Reference() *SkipListElementReference {
	if node == nil {
		return nil
	}

	return &SkipListElementReference{
		ElementPointer: node.GetId(),
		Key:            node.GetKey(),
	}
}

func (t *SkipList) SaveElement(element *SkipListElement) error {
	if element == nil {
		return nil
	}

	return t.ListStore.SaveElement(element.GetId(), element)
}

func (t *SkipList) DeleteElement(element *SkipListElement) error {
	if element == nil {
		return nil
	}

	return t.ListStore.DeleteElement(element.GetId())
}

func (t *SkipList) LoadElement(ref *SkipListElementReference) (*SkipListElement, error) {
	if ref.IsNil() {
		return nil, nil
	}

	return t.ListStore.LoadElement(ref.GetElementPointer())
}

func (ref *SkipListElementReference) IsNil() bool {
	if ref == nil {
		return true
	}
	if len(ref.GetKey()) == 0 {
		return true
	}

	return false
}
