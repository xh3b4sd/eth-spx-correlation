package slicer

type Slicer struct {
	His int
	Lis []float64
}

func (s *Slicer) Add(f float64) {
	{
		s.Lis = append(s.Lis, f)
	}

	if len(s.Lis) > s.His {
		copy(s.Lis[0:], s.Lis[1:])
		s.Lis[len(s.Lis)-1] = 0
		s.Lis = s.Lis[:len(s.Lis)-1]
	}
}

func (s *Slicer) Red() bool {
	return len(s.Lis) == s.His
}
