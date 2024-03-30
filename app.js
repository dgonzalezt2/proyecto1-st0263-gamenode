const express = require('express');
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
    const nodeInfo = { storageCapacityInMb: storageCapacityInMb };
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

    // Filtra los nodos que están en línea antes de proceder
    const onlineNodeIPs = Object.keys(nodes).filter(nodeIP => nodes[nodeIP].online);
    const nodeCapacities = onlineNodeIPs.map(ip => nodes[ip].storageCapacityInMb);

    if (onlineNodeIPs.length < 2) { // Asegura al menos dos nodos en línea para replicación
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

    res.json(chunkDistribution);
});

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
