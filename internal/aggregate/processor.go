package aggregate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type ProcessorConfig struct {
	Workers int
	IngestQ <-chan Event
	Store   *Store

	Outbound interface {
		Publish(any)
	}
}

type Processor struct {
	cfg ProcessorConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	processed uint64
	dropped   uint64
}

func NewProcessor(cfg ProcessorConfig) *Processor {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Processor{cfg: cfg, ctx: ctx, cancel: cancel}
}

func (p *Processor) Start() {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Processor) Stop() {
	p.cancel()
	p.wg.Wait()
}

func (p *Processor) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case e, ok := <-p.cfg.IngestQ:
			if !ok {
				return
			}
			agg, ok := p.cfg.Store.Add(e)
			if !ok {
				atomic.AddUint64(&p.dropped, 1)
				continue
			}
			atomic.AddUint64(&p.processed, 1)
			p.cfg.Outbound.Publish(agg)
		}
	}
}

type Stats struct {
	Processed uint64        `json:"processed"`
	Dropped   uint64        `json:"dropped"`
	Uptime    time.Duration `json:"uptime"`
}

func (p *Processor) Stats(start time.Time) Stats {
	return Stats{
		Processed: atomic.LoadUint64(&p.processed),
		Dropped:   atomic.LoadUint64(&p.dropped),
		Uptime:    time.Since(start),
	}
}

