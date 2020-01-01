package utils

import (
	"os"
	"time"

	"github.com/wcharczuk/go-chart"
)

func PlotMemory(memory []uint64, total []int, out string) error {
	series := chart.ContinuousSeries{
		XValues: []float64{},
		YValues: []float64{},
		Style: chart.Style{
			FillColor: chart.GetDefaultColor(0),
		},
	}
	for i := range memory {
		series.YValues = append(series.YValues, float64(memory[i])/1024/1024)
		series.XValues = append(series.XValues, float64(total[i])/1000)
	}
	f, _ := os.Create(out + ".png")
	defer f.Close()
	graph := chart.Chart{
		Width:  800,
		Height: 400,
		XAxis: chart.XAxis{
			Name: "Entries (thousands)",
		},
		YAxis: chart.YAxis{
			Name: "Memory (MBs)",
		},
		Series: []chart.Series{series},
	}
	return graph.Render(chart.PNG, f)
}

func PlotTimeSpent(duration []time.Duration, total []int, out string) error {
	series := chart.ContinuousSeries{
		XValues: []float64{},
		YValues: []float64{},
		Style: chart.Style{
			FillColor: chart.GetDefaultColor(0),
		},
	}
	for i := range duration {
		series.YValues = append(series.YValues, duration[i].Seconds())
		series.XValues = append(series.XValues, float64(total[i])/1000)
	}
	f, _ := os.Create(out + ".png")
	defer f.Close()
	graph := chart.Chart{
		Width:  800,
		Height: 400,
		XAxis: chart.XAxis{
			Name: "Entries (thousands)",
		},
		YAxis: chart.YAxis{
			Name:           "Seconds",
			ValueFormatter: chart.FloatValueFormatter,
		},
		Series: []chart.Series{series},
	}
	return graph.Render(chart.PNG, f)
}
