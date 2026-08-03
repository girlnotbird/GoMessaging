package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gdamore/tcell/v2"
	shared "github.com/girlnotbird/gomessaging"
	"github.com/rivo/tview"
)

type model struct {
	viewport  *tview.Flex
	chatbox   *tview.TextView
	inputArea *tview.TextArea
	messages  []string
	client    *Client
}

func main() {
	app := tview.NewApplication()
	model := model{
		viewport:  tview.NewFlex(),
		chatbox:   tview.NewTextView(),
		inputArea: tview.NewTextArea(),
		messages:  []string{""},
		client: &Client{
			name:     "Jessie",
			addr:     "127.0.0.1:" + os.Getenv("PORT"),
			c:        nil,
			Messages: make(chan shared.Message, 1),
		},
	}

	err := model.client.Join(model.client.addr, model.client.name)
	if err != nil {
		log.Fatalf("Failed to join server %s...", model.client.addr)
	}

	go func() {
		for {
			msg, err := model.client.Read()
			if err != nil {
				return
			}

			switch msg := msg.(type) {
			case *shared.MsgSendText:
				model.messages = append(model.messages, msg.Text)
				fmt.Fprintf(model.chatbox, "%s\n", msg.Text)
				model.chatbox.ScrollToEnd()

			default:
				model.client.Close()
				return
			}
		}
	}()

	model.viewport.AddItem(model.chatbox, 0, 1, false)
	model.viewport.AddItem(model.inputArea, 3, 0, true)
	model.viewport.SetDirection(tview.FlexRow)
	model.inputArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEnter:
			// TODO: Submit before clearing text buffer
			model.messages = append(model.messages, model.inputArea.GetText())
			fmt.Fprintf(model.chatbox, "You: %s\n", model.inputArea.GetText())
			model.chatbox.ScrollToEnd()
			model.client.Send(model.inputArea.GetText())
			model.inputArea.SetText("", true)
			return nil
		case tcell.KeyLeft, tcell.KeyRight, tcell.KeyUp, tcell.KeyDown:
			return nil
		default:
			return event
		}
	})
	model.inputArea.SetMaxLength(256)
	model.chatbox.SetChangedFunc(func() { app.Draw() })
	model.chatbox.SetText(fmt.Sprintf("Welcome to %v!\n", model.client.addr))

	app.SetRoot(model.viewport, true)
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
