package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/xdg-go/scram"
)

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type TokenProvider struct {
	clientId string
	secret   string
	url      string
	token    string
	caPath   string
}

func (t *TokenProvider) Token() (*sarama.AccessToken, error) {
	var token sarama.AccessToken

	type tokenResponse struct {
		Token string `json:"auth_token"`
	}
	var respObj tokenResponse
	var tlsConfig *tls.Config

	tlsConfig = createTlsConfig(t.caPath, false)
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	hClient := &http.Client{Transport: transport}
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
	respBody, err := io.ReadAll(resp.Body)
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

func NewTokenProviderConfluentMDS(client, secret, url, caPath string) sarama.AccessTokenProvider {
	tokenprovider := TokenProvider{
		clientId: client,
		secret:   secret,
		url:      url,
		caPath:   caPath,
	}

	return &tokenprovider
}

// SCRAM auth client
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
