package main

import (
	service "github.com/nayotta/metathings-sensor-gossiper/pkg/gossiper/service"
	component "github.com/nayotta/metathings/pkg/component"
)

func main() {
	mdl, err := component.NewModule("gossiper", new(service.GossiperService))
	if err != nil {
		panic(err)
	}

	err = mdl.Launch()
	if err != nil {
		panic(err)
	}
}
