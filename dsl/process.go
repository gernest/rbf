package dsl

func (s *Store[T]) Append(data []T) error {
	schema, err := s.Schema()
	if err != nil {
		return err
	}
	defer schema.Release()
	for i := range data {
		err = schema.Write(data[i])
		if err != nil {
			return err
		}
	}
	return schema.Save()
}
