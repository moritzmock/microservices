import express from "express"
import request from "request-promise-native"
import bodyParser from 'body-parser'
const app = express()

app.use(bodyParser.json());

const domain = "http://0.0.0.0"
const portAppartments = 5001
const portSearch = 5002
const portReserve = 5003
const port = 5000

app.get('/appartments', async (req, res) => {
    const uri = `${domain}:${portAppartments}/appartments`
    const user = await request(uri)
    res.json(user)
})

app.get('/appartments/add', async (req, res) => {
    const params = req.url.split("/")[2]
    const uri = `${domain}:${portAppartments}/${params}`
    try {
        const user = await request(uri)
        res.json(user)
    } catch (e){
        res.json(e.message)
    }
})

app.get('/appartments/remove', async (req, res) => {
    const params = req.url.split("/")[2]
    const uri = `${domain}:${portAppartments}/${params}`
    try {
        const user = await request(uri)
        res.json(user)
    } catch (e){
        res.json(e.message)
    }
})

app.get('/search', async (req, res) => {
    const uri = `${domain}:${portSearch}`
    const user = await request(uri)
    res.json(user)
})

app.get('/reserve', async (req, res) => {
    const uri = `${domain}:${portReserve}`
    const user = await request(uri)
    res.json(user)
})

app.get('/reserve/add', async (req, res) => {
    const params = req.url.split("/")[2]
    const uri = `${domain}:${portReserve}/${params}`
    try {
        const user = await request(uri)
        res.json(user)
    } catch (e){
        res.json(e.message)
    }
})

app.get('/reserve/remove', async (req, res) => {
    const params = req.url.split("/")[2]
    const uri = `${domain}:${portReserve}/${params}`
    try {
        const user = await request(uri)
        res.json(user)
    } catch (e){
        res.json(e.message)
    }
})


app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})
