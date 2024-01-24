package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type requestTimer struct {
	startTime time.Time
	endTime   time.Time
	duration  time.Duration
}

func getRandomHexString(length int) string {
	if length <= 0 {
		return ""
	}

	b := make([]byte, length/2+1)
	_, err := rand.Read(b)
	if err != nil {
		return "error"
	}

	return fmt.Sprintf("%x", b)[:length]
}

func getStats(responseTimes map[string]requestTimer) (time.Duration, time.Duration, time.Duration, int32) {
	total := time.Duration(0)
	min := time.Duration(99999 * time.Hour)
	max := time.Duration(0)
	errorCount := int32(0)

	for _, responseTimer := range responseTimes {
		if responseTimer.duration > max {
			max = responseTimer.duration
		}
		if responseTimer.duration < min && responseTimer.duration != 0 {
			min = responseTimer.duration
		}
		if responseTimer.duration == 0 && responseTimer.endTime == responseTimer.startTime {
			errorCount++
		}
		total += responseTimer.duration
	}
	return min, max, total / time.Duration(len(responseTimes)-int(errorCount)), errorCount
}

func testRate(broker string, port int16, topic string, qos byte, requestCount int, delayms int, globalWg *sync.WaitGroup, resultsMap map[string]requestTimer, mutex *sync.RWMutex) {
	wg := sync.WaitGroup{}

	options := mqtt.NewClientOptions()
	options.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	options.Order = true
	options.SetClientID(topic)
	options.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		if !strings.HasPrefix(string(msg.Payload()), "res_att") {
			return
		}
		response := strings.Split(string(msg.Payload()), ",")
		randomId := response[len(response)-1]
		mutex.Lock()
		responseTimer, ok := resultsMap[randomId]
		if !ok {
			fmt.Println("Error: response received for unknown request")
			mutex.Unlock()
			wg.Done()
			return
		}
		responseTimer.endTime = time.Now()
		responseTimer.duration = responseTimer.endTime.Sub(responseTimer.startTime)
		resultsMap[randomId] = responseTimer
		mutex.Unlock()
		wg.Done()
	})
	options.OnConnect = func(client mqtt.Client) {
		fmt.Printf("\rConnected to broker. Client: %v\n", topic)
	}
	options.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost on clientId %s. err: %s\n", topic, err)
	}

	client := mqtt.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	token := client.Subscribe(topic, qos, nil)
	token.Wait()

	for i := 0; i < requestCount; i++ {
		randomId := getRandomHexString(8)
		mutex.Lock()
		resultsMap[randomId] = requestTimer{time.Now(), time.Now(), time.Duration(0)}
		mutex.Unlock()
		wg.Add(1)
		client.Publish(topic, qos, false, fmt.Sprint("req_att,", randomId))
		wg.Wait()
		time.Sleep((time.Duration(delayms) * time.Millisecond) - resultsMap[randomId].duration)
	}

	defer func() {
		client.Disconnect(0)
		globalWg.Done()
	}()
}

func singleClientTest(broker string, port int16, topic string, qos byte, numOfRequests int, delayms int, rateStep int, resultsMap map[string]requestTimer, wg *sync.WaitGroup, mutex *sync.RWMutex, file *os.File) {
	for {
		localWg := sync.WaitGroup{}
		localMutex := sync.RWMutex{}

		responseTimes := make(map[string]requestTimer)
		fmt.Println()
		spin := true
		go func() {
			for spin {
				for _, r := range `-\|/` {
					if delayms > 1 {
						fmt.Printf("\rTesting on %vreq/s with %v requests %c ", 1000/delayms, numOfRequests, r)
					} else {
						fmt.Printf("\rTesting max req/s with %v requests %c ", numOfRequests, r)
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		}()

		localWg.Add(1)
		startTime := time.Now()
		go testRate(broker, int16(port), topic, qos, numOfRequests, delayms, &localWg, responseTimes, &localMutex)
		localWg.Wait()
		duration := time.Since(startTime)

		spin = false

		min, max, avg, errorCount := getStats(responseTimes)
		fmt.Printf("\nMin: %s, Max: %s, Avg: %s\n", min, max, avg)
		fmt.Printf("Time: %s\n", duration)
		fmt.Printf("Error count: %d\n", errorCount)

		if mutex != nil {
			mutex.Lock()
		}

		if delayms > 1 {
			file.WriteString(fmt.Sprintf("\nDevice: %v, Testing on %vreq/s with %v requests\n", topic, 1000/delayms, numOfRequests))
		} else {
			file.WriteString(fmt.Sprintf("\nDevice: %v, Testing max req/s with %v requests\n", topic, numOfRequests))
		}
		file.WriteString(fmt.Sprintf("\nMin: %s, Max: %s, Avg: %s\n", min, max, avg))
		file.WriteString(fmt.Sprintf("Time: %s\n", duration))
		file.WriteString(fmt.Sprintf("Error count: %d\n", errorCount))

		for k, v := range responseTimes {
			resultsMap[k] = v
			file.WriteString(fmt.Sprintf("Id:%s, StartTime: %s, EndTime%s, Duration: %s\n", k, v.startTime, v.endTime, v.duration))
		}
		if mutex != nil {
			mutex.Unlock()
		}
		if delayms <= 1 {
			break
		}

		delayms /= rateStep
	}

	if wg != nil {
		wg.Done()
	}
}

func main() {
	var input string
	f, err := os.Create("result.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	broker := "192.168.10.178"
	port := 1883
	qos := byte(0)

	rateStep := 2
	numOfRequests := 1000
	initialDelayms := 500
	responseTimesTotal := make(map[string]requestTimer)

	fmt.Println("Testing single client request rate")
	singleClientTestStartTime := time.Now()
	singleClientTest(broker, int16(port), "Device1", qos, numOfRequests, initialDelayms, rateStep, responseTimesTotal, nil, nil, f)
	singleClientTestDuration := time.Since(singleClientTestStartTime)

	min, max, avg, errorCount := getStats(responseTimesTotal)
	fmt.Println("\nSingle client request rate test done.")
	fmt.Printf("Total time: %s\n", singleClientTestDuration)
	fmt.Printf("Total requests: %d\n", len(responseTimesTotal))
	fmt.Printf("Total error count: %d\n", errorCount)
	fmt.Printf("Min: %s, Max: %s, Avg: %s\n", min, max, avg)
	fmt.Printf("Max Rate: ~%vreq/s\n", int(time.Second/avg))
	fmt.Printf("Max troughput: ~%vbytes/s\n", int(time.Second/avg)*53)

	f.WriteString("\nSingle client request rate test done.\n")
	f.WriteString(fmt.Sprintf("Total time: %s\n", singleClientTestDuration))
	f.WriteString(fmt.Sprintf("Total requests: %d\n", len(responseTimesTotal)))
	f.WriteString(fmt.Sprintf("Total error count: %d\n", errorCount))
	f.WriteString(fmt.Sprintf("Min: %s, Max: %s, Avg: %s\n", min, max, avg))
	f.WriteString(fmt.Sprintf("Max Rate: ~%vreq/s\n", int(time.Second/avg)))
	f.WriteString(fmt.Sprintf("Max troughput: ~%vbytes/s\n", int(time.Second/avg)*53))

	fmt.Println("\nTesting multiple clients request rate")
	f.WriteString("\nTesting multiple clients request rate")
	responseTimesTotal = make(map[string]requestTimer)
	wgMultiClientTest := sync.WaitGroup{}
	mutexMultiClientTest := sync.RWMutex{}
	multiClientTestStartTime := time.Now()
	numOfRequests = 100
	numOfClients := 2
	newNumClientFactor := 2

	for {
		subMultiClientTestStartTime := time.Now()
		fmt.Printf("\nTesting with %v clients\n", numOfClients)
		f.WriteString(fmt.Sprintf("\nTesting with %v clients\n", numOfClients))

		for i := 1; i < numOfClients+1; i++ {
			wgMultiClientTest.Add(1)
			go singleClientTest(broker, int16(port), fmt.Sprintf("Device%d", i), qos, numOfRequests, 0, rateStep, responseTimesTotal, &wgMultiClientTest, &mutexMultiClientTest, f)
		}
		wgMultiClientTest.Wait()

		fmt.Printf("\nMulti client request rate with %v clients done.\n", numOfClients)
		fmt.Printf("Total time: %s\n", time.Since(subMultiClientTestStartTime))
		fmt.Printf("Total requests: %d\n", len(responseTimesTotal))
		fmt.Printf("Total error count: %d\n", errorCount)
		fmt.Printf("Min: %s, Max: %s, Avg: %s\n", min, max, avg)
		fmt.Printf("Max Rate: ~%vreq/s\n", int(time.Second/avg))
		fmt.Printf("Max troughput: ~%vbytes/s\n", int(time.Second/avg)*53)

		f.WriteString(fmt.Sprintf("\nMulti client request rate with %v clients done.\n", numOfClients))
		f.WriteString(fmt.Sprintf("Total time: %s\n", time.Since(subMultiClientTestStartTime)))
		f.WriteString(fmt.Sprintf("Total requests: %d\n", len(responseTimesTotal)))
		f.WriteString(fmt.Sprintf("Total error count: %d\n", errorCount))
		f.WriteString(fmt.Sprintf("Min: %s, Max: %s, Avg: %s\n", min, max, avg))
		f.WriteString(fmt.Sprintf("Max Rate: ~%vreq/s\n", int(time.Second/avg)))
		f.WriteString(fmt.Sprintf("Max troughput: ~%vbytes/s\n", int(time.Second/avg)*53))

		numOfClients *= newNumClientFactor
		if numOfClients > 128 {
			break
		}
	}

	multiClientTestDuration := time.Since(multiClientTestStartTime)
	min, max, avg, errorCount = getStats(responseTimesTotal)
	fmt.Printf("\nMulti client request rate test done.\n")
	fmt.Printf("Total time: %s\n", multiClientTestDuration)
	fmt.Printf("Total requests: %d\n", len(responseTimesTotal))
	fmt.Printf("Total error count: %d\n", errorCount)
	fmt.Printf("Min: %s, Max: %s, Avg: %s\n", min, max, avg)
	fmt.Printf("Max Rate: ~%vreq/s\n", int(time.Second/avg))
	fmt.Printf("Max troughput: ~%vbytes/s\n", int(time.Second/avg)*53)

	f.WriteString("\nMulti client request rate test done.\n")
	f.WriteString(fmt.Sprintf("Total time: %s\n", multiClientTestDuration))
	f.WriteString(fmt.Sprintf("Total requests: %d\n", len(responseTimesTotal)))
	f.WriteString(fmt.Sprintf("Total error count: %d\n", errorCount))
	f.WriteString(fmt.Sprintf("Min: %s, Max: %s, Avg: %s\n", min, max, avg))
	f.WriteString(fmt.Sprintf("Max Rate: ~%vreq/s\n", int(time.Second/avg)*numOfClients))
	f.WriteString(fmt.Sprintf("Max troughput: ~%vbytes/s\n", int(time.Second/avg)*53*numOfClients))

	time.Sleep(10 * time.Second)

	for input != "exit" {
		fmt.Println("\nTesting done. type 'exit' to exit")
		fmt.Scanln(&input)
	}
}
