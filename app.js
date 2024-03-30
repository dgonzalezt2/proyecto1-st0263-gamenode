\const express = require('express');
const jsonStream = require('JSONStream');
const fs = require('fs');
const fetch = require('node-fetch');
const cors = require('cors');

const app = express();
const port = 3000;
const NODES_FILE_PATH = process.env.NODE_LOCATION || './nodes.json';
const FILES_FILE_PATH = process.env.FILES_LOCATION || './files.json';

app.use(express.json());
app.use(cors({
    origin: '*',
    credentials: true,
    methods: '*',
    allowedHeaders: '*'
}));
function fileExists(path) {
    try {
        fs.accessSync(path, fs.constants.F_OK);
        return true;
    } catch (err) {
        return false;
    }
}

app.post('/add-node', (req, res) => {
    const { storageCapacityInMb, files } = req.body;

    const nodesFilePath = NODES_FILE_PATH;
    const nodeIP = req.ip;
    const nodeInfo = { storageCapacityInMb: storageCapacityInMb, online: true };
    updateNodeInfo(nodesFilePath, nodeIP, nodeInfo);

    const filesFilePath = FILES_FILE_PATH;
    updateFilesInfo(filesFilePath, files, nodeIP);
    console.log("create new node")
    res.status(200).json({ success: true });
});

function updateNodeInfo(filePath, nodeIP, nodeInfo) {
    let nodes = {};
    if (fileExists(filePath)) {
        const nodesData = fs.readFileSync(filePath, 'utf8');
        nodes = JSON.parse(nodesData);
    }
    nodes[nodeIP] = nodeInfo;
    fs.writeFileSync(filePath, JSON.stringify(nodes, null, 2));
}

function updateFilesInfo(filePath, files, nodeIP) {
    let existingFiles = {};
    if (fileExists(filePath)) {
        const filesData = fs.readFileSync(filePath, 'utf8');
        existingFiles = JSON.parse(filesData);
    }
    for (const [fileName, fileInfo] of Object.entries(files)) {
        if (!existingFiles[fileName]) {
            existingFiles[fileName] = { chunks: {} };
        }
        fileInfo.availableChunks.forEach(chunk => {
            if (!existingFiles[fileName].chunks[chunk]) {
                existingFiles[fileName].chunks[chunk] = [];
            }
            if (!existingFiles[fileName].chunks[chunk].includes(nodeIP)) {
                existingFiles[fileName].chunks[chunk].push(nodeIP);
            }
        });
    }
    fs.writeFileSync(filePath, JSON.stringify(existingFiles, null, 2));
}


function calculateChunkSize(fileSizeInMb) {
    if (fileSizeInMb <= 1024) {
        return 128;
    } else if (fileSizeInMb <= 10240) {
        return 256;
    } else {
        return 512;
    }
}

app.get('/chunk-file', (req, res) => {
    const { fileSizeInMb } = req.query;
    if (!fileSizeInMb) {
        return res.status(400).json({ error: "fileSizeInMb query parameter is required" });
    }

    const fileSize = parseInt(fileSizeInMb, 10);
    const chunkSize = calculateChunkSize(fileSize);
    const totalChunks = Math.ceil(fileSize / chunkSize);

    const nodesFilePath = NODES_FILE_PATH;
    let nodes = {};
    if (fileExists(nodesFilePath)) {
        const nodesData = fs.readFileSync(nodesFilePath, 'utf8');
        nodes = JSON.parse(nodesData);
    } else {
        return res.status(500).json({ error: "Node information not found" });
    }

    const onlineNodeIPs = Object.keys(nodes).filter(nodeIP => nodes[nodeIP].online);
    const nodeCapacities = onlineNodeIPs.map(ip => nodes[ip].storageCapacityInMb);

    if (onlineNodeIPs.length < 2) {
        return res.status(507).json({ error: "Insufficient online nodes for replication (minimum required: 2)" });
    }

    let chunkDistribution = {};
    for (let i = 0; i < totalChunks; i++) {
        let assigned = false;
        for (let j = 0; j < onlineNodeIPs.length && !assigned; j++) {
            const nodeIndex = (i + j) % onlineNodeIPs.length;
            const nodeIP = onlineNodeIPs[nodeIndex];
            if (nodeCapacities[nodeIndex] >= chunkSize) {
                nodeCapacities[nodeIndex] -= chunkSize;
                if (!chunkDistribution[nodeIP]) {
                    chunkDistribution[nodeIP] = [];
                }
                chunkDistribution[nodeIP].push(i + 1);
                assigned = true;
            }
        }
        if (!assigned) {
            return res.status(507).json({ error: "Insufficient Storage Capacity among online nodes" });
        }
    }
    res.json({
        chunkDistribution,
        totalChunks,
        chunkSize
    });
});

app.post('/add-chunk', (req, res) => {
    const { chunkId, fileName } = req.body;
    if (!chunkId || !fileName) {
        return res.status(400).json({ error: "chunkId and fileName are required" });
    }

    const nodeIP = req.ip;
    addChunkToFile(FILES_FILE_PATH, fileName, chunkId, nodeIP);

    res.status(200).json({ success: true, message: `Chunk ${chunkId} added to file ${fileName}.` });
});

function addChunkToFile(filePath, fileName, chunkId, nodeIP) {
    let files = {};
    if (fileExists(filePath)) {
        const filesData = fs.readFileSync(filePath, 'utf8');
        files = JSON.parse(filesData);
    }
    if (!files[fileName]) {
        files[fileName] = { chunks: {} };
    }
    if (!files[fileName].chunks[chunkId]) {
        files[fileName].chunks[chunkId] = [];
    }

    if (!files[fileName].chunks[chunkId].includes(nodeIP)) {
        files[fileName].chunks[chunkId].push(nodeIP);
    }
    fs.writeFileSync(filePath, JSON.stringify(files, null, 2));
}

app.post('/update-file', (req, res) => {
    const { fileName, chunkSize, totalChunks } = req.body;

    if (!fileName || !chunkSize || !totalChunks) {
        return res.status(400).json({ error: "fileName, chunkSize and totalChunks are required" });
    }

    const updated = updateFileInfo(FILES_FILE_PATH, fileName, chunkSize, totalChunks);
    if (updated) {
        res.status(200).json({ success: true, message: `File ${fileName} updated successfully.` });
    } else {
        res.status(500).json({ success: false, message: "Error updating file information." });
    }
});

function updateFileInfo(filePath, fileName, chunkSize, totalChunks) {
    let filesData = {};

    if (fileExists(filePath)) {
        const data = fs.readFileSync(filePath, 'utf8');
        filesData = JSON.parse(data);
    }

    if (!filesData[fileName]) {
        filesData[fileName] = { chunks: {}, chunkSize: chunkSize, totalChunks: totalChunks };
    } else {
        filesData[fileName].chunkSize = chunkSize;
        filesData[fileName].totalChunks = totalChunks;
    }

    try {
        fs.writeFileSync(filePath, JSON.stringify(filesData, null, 2));
        return true;
    } catch (error) {
        console.error("Error writing to files.json:", error);
        return false;
    }
}

app.get('/get-file', (req, res) => {
    const fileName = req.query.fileName;

    if (!fileName) {
        return res.status(400).json({ error: "fileName query parameter is required" });
    }

    const fileInfo = getFileInfo(FILES_FILE_PATH, fileName);
    if (fileInfo) {
        res.json(fileInfo);
    } else {
        res.status(404).json({ error: `File ${fileName} not found.` });
    }
});

function getFileInfo(filePath, fileName) {
    if (fileExists(filePath)) {
        const filesData = fs.readFileSync(filePath, 'utf8');
        const files = JSON.parse(filesData);
        if (files[fileName]) {
            return files[fileName];
        }
    }
    return null;
}

async function pingNode(nodeIP) {
    try {
        const response = await fetch(`http://${nodeIP}/ping`);
        return response.ok;
    } catch (error) {
        return false;
    }
}

async function pingNodes() {
    console.log("ping to nodes")
    let nodes = {};
    const nodesFilePath = NODES_FILE_PATH;
    if (fileExists(nodesFilePath)) {
        const nodesData = fs.readFileSync(nodesFilePath, 'utf8');
        nodes = JSON.parse(nodesData);
    } else {
        console.log("No se encontraron nodos para hacer ping.");
        return;
    }

    const pingPromises = Object.keys(nodes).map(async (nodeIP) => {
        const online = await pingNode(nodeIP);
        nodes[nodeIP].online = online;
    });

    await Promise.all(pingPromises);

    fs.writeFileSync(nodesFilePath, JSON.stringify(nodes, null, 2));
}

app.listen(port, '0.0.0.0',() => {
    console.log(`API escuchando en el puerto ${port}`);
    if (!fileExists(NODES_FILE_PATH)) {
        fs.writeFileSync(NODES_FILE_PATH, '{}');
    }
    if (!fileExists(FILES_FILE_PATH)) {
        fs.writeFileSync(FILES_FILE_PATH, '{}');
    }
    setInterval(pingNodes, 5000);
});