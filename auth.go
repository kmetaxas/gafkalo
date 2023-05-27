package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type TokenProvider struct {
	clientId string
	secret   string
	url      string
	token    string
}

func (t *TokenProvider) Token() (*sarama.AccessToken, error) {
	var token sarama.AccessToken

	type tokenResponse struct {
		Token string `json:"auth_token"`
	}
	var respObj tokenResponse

	hClient := http.Client{}
	req, err := http.NewRequest("GET", t.url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.SetBasicAuth(t.clientId, t.secret)
	req.Header.Add("Content-Type", "application/json")
	resp, err := hClient.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
	}

	log.Tracef("MDS replied with %s", respBody)
	err = json.Unmarshal(respBody, &respObj)
	if err != nil {
		log.Fatalf("Failed unmarshaling response from token: %s\n", err)
	}
	log.Tracef("Unmarshaled token into %v", respObj)
	// Now parse into Token and return
	t.token = string(respObj.Token)
	token.Token = t.token
	log.Tracef("Token provider has token %s", t.token)

	return &token, nil
}
func NewTokenProviderConfluentMDS(client, secret, url string) sarama.AccessTokenProvider {
	tokenprovider := TokenProvider{
		clientId: client,
		secret:   secret,
		url:      url,
	}

	return &tokenprovider
}
