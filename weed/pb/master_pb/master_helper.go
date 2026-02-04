package master_pb

func (v *VolumeLocation) IsEmptyUrl() bool {
	return v.GetUrl() == "" || v.GetUrl() == ":0"
}
