package cmd

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/spf13/cobra"
)

func init() { Root.AddCommand(NewCmdRoundtrip()) }

func NewCmdRoundtrip() *cobra.Command {
	var sizeScale, layerScale, repeats int
	var output string
	roundtrip := &cobra.Command{
		Use:   "roundtrip",
		Short: "Generate random images and push/pull",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			registry := args[0]
			rb := &roundtripBench{
				registry:   registry,
				output:     output,
				sizeScale:  int64(sizeScale),
				layerScale: int64(layerScale),
				repeats:    int64(repeats),
			}
			rb.run()
		},
	}
	roundtrip.Flags().IntVarP(&sizeScale, "size_scale", "S", 10, "Initial size = 1k and the scale is applied until reach 1G.")
	roundtrip.Flags().IntVarP(&layerScale, "layer_scale", "L", 2, "Initial number of layers = 1 and the scale is applied until reach 32 layers.")
	roundtrip.Flags().IntVarP(&repeats, "repeat_factor", "R", 1, "For each size+layer combination, how many repeats.")
	roundtrip.Flags().StringVarP(&output, "output", "O", "output", "The result output file.")
	return roundtrip
}

type roundtripBench struct {
	registry, output               string
	sizeScale, layerScale, repeats int64
}

func (rb *roundtripBench) run() {
	sizeBase := int64(1024)
	maxSize := sizeBase * sizeBase * sizeBase
	var iter int64

	res, err := os.OpenFile(rb.output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	printOutput(res, "Num,Size,Layers,Push,Pull\n")

	for size := sizeBase; size <= maxSize; size = size * rb.sizeScale {
		for layer := int64(1); layer <= 32; layer = layer * rb.layerScale {
			for r := int64(0); r < rb.repeats; r++ {
				dst := fmt.Sprintf("%s/loadline:%d-%d", rb.registry, time.Now().Nanosecond(), iter)

				dstTag, err := name.NewTag(dst, name.WeakValidation)
				if err != nil {
					log.Fatalf("parsing tag %q: %v", dst, err)
				}

				img, err := random.Image(size, layer)
				if err != nil {
					log.Fatalf("random.Image: %v", err)
				}

				writeStart := time.Now()
				if err := remote.Write(dstTag, img, remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
					log.Fatalf("writing image %q: %v", dstTag, err)
				}
				writeElapsed := time.Since(writeStart) / time.Millisecond

				readStart := time.Now()
				rimg, err := crane.Pull(dst)
				if err != nil {
					log.Fatal(err)
				}
				if err := crane.Export(rimg, &DiscardCloser{ioutil.Discard}); err != nil {
					log.Fatalf("exporting %s: %v", dst, err)
				}
				readElapsed := time.Since(readStart) / time.Millisecond

				printOutput(res, fmt.Sprintf("%d,%d,%d,%d,%d\n", iter, size, layer, writeElapsed, readElapsed))

				iter++
			}
		}
	}

	if err := res.Close(); err != nil {
		log.Fatal(err)
	}
}

func printOutput(f *os.File, d string) {
	if _, err := f.WriteString(d); err != nil {
		log.Fatal(err)
	}
}

type DiscardCloser struct {
	io.Writer
}

func (DiscardCloser) Close() error { return nil }
