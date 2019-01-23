package slices

func RemoveItemByIndex(set []interface{}, i int) {
	if i >= len(set) {
		set = set[0 : len(set)-1]
	} else {
		set = append(set[0:i], set[i+1:]...)
	}
}
