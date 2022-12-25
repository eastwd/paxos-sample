package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Proposer struct {
	ProposeId    int
	ProposeValue int
	Acceptors    []*Acceptor
}

func NewProposer(proposeId, proposeValue int, acceptors []*Acceptor) *Proposer {
	return &Proposer{ProposeId: proposeId, ProposeValue: proposeValue, Acceptors: acceptors}
}

func (p *Proposer) Propose(proposeValue int) {
	p.ProposeValue = proposeValue
	fmt.Printf("Propose is starting: ProposeID=%d ProposeValue=%d\n", p.ProposeId, p.ProposeValue)
	for {
		time.Sleep(time.Duration(5) * time.Second)
		fmt.Printf("send prepare request: PrepareId=%d ProposeValue=%d\n", p.ProposeId, p.ProposeValue)
		responses := []PrepareResponse{}
		for _, acceptor := range p.Acceptors {
			responses = append(responses, acceptor.Prepare(p.ProposeId))
		}
		restartFlag := false
		for _, response := range responses {
			if response.Err != nil {
				fmt.Printf("Prepare faces to error at ServerId=%s\n", response.ServerId)
				continue
			}
			if response.Answer == NG {
				p.ProposeId = p.ProposeId + 1
				restartFlag = true
				fmt.Printf("Prepare is rejected by ServerId=%s\n", response.ServerId)
				break
			}
		}
		if restartFlag {
			continue
		}
		fmt.Println("ok")
		count := 0
		fmt.Println("check prepare response")
		for _, response := range responses {
			if response.Err == nil {
				count++
			}
		}
		if count < len(responses) {
			continue
		}
		fmt.Println("ok")
		fmt.Println("send propose request")
		maxProposeId := p.ProposeId
		for _, response := range responses {
			if response.Err != nil {
				continue
			}
			if response.ProposeValue == 0 {
				continue
			}
			if maxProposeId < response.ProposeId {
				maxProposeId = response.ProposeId
				p.ProposeValue = response.ProposeValue
			}
		}
		success := true
		for _, acceptor := range p.Acceptors {
			if success {
				success = acceptor.Propose(p.ProposeId, p.ProposeValue)
			}
		}
		fmt.Println("ok")
		if success {
			break
		}
		fmt.Println("wait next proposing...")
	}
}

type Acceptor struct {
	ServerId     string
	ProposeId    int
	ProposeValue int
}

func NewAcceptor(proposeId, proposeValue int) *Acceptor {
	return &Acceptor{ProposeId: proposeId, ProposeValue: proposeValue}
}

type Answer string

const (
	OK Answer = "ok"
	NG Answer = "ng"
)

type PrepareResponse struct {
	Answer       Answer
	ServerId     string
	ProposeId    int
	ProposeValue int
	Err          error
}

func (a *Acceptor) Prepare(proposeId int) PrepareResponse {
	fmt.Printf("[%s] Prepare is called: ProposeId=%d\n", a.ServerId, proposeId)
	if rand.Intn(10) <= 1 {
		fmt.Printf("[%s] error occuer: ProposeId=%d ProposeValue=%d\n", a.ServerId, a.ProposeId, a.ProposeValue)
		return PrepareResponse{Answer: OK, ServerId: a.ServerId, ProposeId: proposeId, ProposeValue: a.ProposeValue, Err: fmt.Errorf("error occuer at %s", a.ServerId)}
	}
	if a.ProposeId < proposeId {
		fmt.Printf("[%s] proposeId is updated: ProposeId=%d ProposeValue=%d\n", a.ServerId, a.ProposeId, a.ProposeValue)
		return PrepareResponse{Answer: OK, ServerId: a.ServerId, ProposeId: proposeId, ProposeValue: a.ProposeValue, Err: nil}
	}
	fmt.Printf("[%s] proposeId isn't updated: ProposeId=%d ProposeValue=%d\n", a.ServerId, a.ProposeId, a.ProposeValue)
	return PrepareResponse{Answer: NG, ServerId: a.ServerId, ProposeId: a.ProposeId, ProposeValue: a.ProposeValue, Err: nil}
}

func (a *Acceptor) Propose(proposeId, proposeValue int) bool {
	if a.ProposeId <= proposeId {
		a.ProposeId = proposeId
		a.ProposeValue = proposeValue
		fmt.Printf("[%s] propose is accepted: ProposeId=%d, ProposeValue=%d\n", a.ServerId, a.ProposeId, a.ProposeValue)
		return true
	}
	fmt.Printf("[%s] propose is rejected: ProposeId=%d, ProposeValue=%d\n", a.ServerId, a.ProposeId, a.ProposeValue)
	return false
}

func main() {

	seed := time.Now().UnixNano()
	rand.Seed(seed)

	acceptors := []*Acceptor{
		{
			ServerId:     "acceptor1",
			ProposeId:    100,
			ProposeValue: 20,
		},
		{
			ServerId:     "acceptor2",
			ProposeId:    110,
			ProposeValue: 30,
		},
		{
			ServerId:     "acceptor3",
			ProposeId:    120,
			ProposeValue: 40,
		},
	}
	proposer := NewProposer(115, 1, acceptors)
	proposer.Propose(2)
	proposer.ProposeId++
	proposer.Propose(111000)
}
