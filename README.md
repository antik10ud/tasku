# tasku



NOT FOR PRODUCTION USE

simple persistent distributed(rethinkdb backend) task queue 

    qs, err := NewQueueService(
            &Opts{
                Addresses: []string{"localhost:28015"}, // rethinkdb address
                Db:        "queues",
            })
	
	defer qs.Stop()
    
    
Simple use

Push message/job/task

    qs.Send("test", Message{ Payload: "Payload"})    
                

Consumer with retry custom retry strategy

    retryOpts := queues.RetryOpts{RetryPlan: []time.Duration{60 * time.Minute, 120 * time.Minute, 240 * time.Minute}}
   
   	c1, err := queues.WithRetryStrategy(queuesService, "test", queues.DefaultReceiveOpts, retryOpts, consumerFunc)

    <-c1
   
   
    func consumerFunc(q queues.Queue, m *queues.Message) error {
        ...
        
        
	}
                   
Factory reset and queue creation

    	qs.ResetToFactory()
    	qs.Create("test")

Start custom queue consumer
  
    	planner, err := qs.Receive("test", ReceiveOpts{Parallel: 5})
      
    	go func() {
    		for {
    			select {
    			case m := <-planner.C:
    				qs.Delete("test", m.Id)
    				break
    			}
    		}
    	}()
   
    	