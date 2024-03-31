const express = require('express');
const jsonStream = require('JSONStream');
const fs = require('fs');
const fetch = require('node-fetch');
const cors = require('cors');

const app = express();
const port = 3000;
const NODES_FILE_PATH = process.env.NODE_LOCATION || './nodes.json';
const FILES_FILE_PATH = process.env.FILES_LOCATION || './files.json';

let nodes = {};
let files = {};

if (fs.existsSync(NODES_FILE_PATH)) {
    const nodesData = fs.readFileSync(NODES_FILE_PATH, 'utf8');
    nodes = JSON.parse(nodesData);
}

if (fs.existsSync(FILES_FILE_PATH)) {
    const filesData = fs.readFileSync(FILES_FILE_PATH, 'utf8');
    files = JSON.parse(filesData);
}

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

function updateNodeInfo(filePath, nodeIP, nodeInfo) {
    nodes[nodeIP] = nodeInfo;
    fs.writeFileSync(filePath, JSON.stringify(nodes, null, 2));
}

function updateFilesInfo(filePath, filesData) {
    files = filesData;
    fs.writeFileSync(filePath, JSON.stringify(files, null, 2));
}

function updateNodeStorageCapacity(nodeIP, newFreeStorage) {
    if (nodes[nodeIP]) {
        nodes[nodeIP].storageCapacityInMb = newFreeStorage;
        fs.writeFileSync(NODES_FILE_PATH, JSON.stringify(nodes, null, 2));
    }
}

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

async function pingNodes() {
    console.log("ping to nodes");
    let updated = false;
    const nodesFilePath = NODES_FILE_PATH;
    if (fileExists(nodesFilePath)) {
        const pingPromises = Object.keys(nodes).map(async (nodeIP) => {
            const online = await pingNode(nodeIP);
            if (nodes[nodeIP].online !== online) {
                nodes[nodeIP].online = online;
                updated = true;
            }
        });

        await Promise.all(pingPromises);

        if (updated) {
            fs.writeFileSync(nodesFilePath, JSON.stringify(nodes, null, 2));
        }
    } else {
        console.log("No se encontraron nodos para hacer ping.");
    }
}

async function pingNode(nodeIP) {
    try {
        const response = await fetch(`http://${nodeIP}/ping`);
        return response.ok;
    } catch (error) {
        return false;
    }
}

app.post('/add-node', (req, res) => {
    const { storageCapacityInMb, files } = req.body;

    const nodeIP = req.ip;
    const nodeInfo = { storageCapacityInMb: storageCapacityInMb, online: true };
    updateNodeInfo(NODES_FILE_PATH, nodeIP, nodeInfo);

    updateFilesInfo(FILES_FILE_PATH, files);
    console.log("create new node")
    res.status(200).json({ success: true });
});

function calculateChunkSize(fileSizeInMb) {
    if (fileSizeInMb <= 128) {
        return fileSizeInMb;
    }
    else if (fileSizeInMb <= 1024) {
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

    const fileSize = parseFloat(fileSizeInMb);
    const chunkSize = calculateChunkSize(fileSize);
    const totalChunks = Math.max(Math.ceil(fileSize / chunkSize), 1);

    let nodes = {};
    if (fileExists(NODES_FILE_PATH)) {
        const nodesData = fs.readFileSync(NODES_FILE_PATH, 'utf8');
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

/**
 * Verifica si todas las llaves requeridas están presentes en el objeto.
 * @param {Object} obj - El objeto a verificar.
 * @param {Array} requiredKeys - Las llaves requeridas.
 * @returns {Array} - Un array vacío si todas las llaves existen, o un array con las llaves faltantes.
 */
function checkRequiredKeys(obj, requiredKeys) {
    return requiredKeys.filter(key => !(key in obj));
}

app.post('/add-chunk', (req, res) => {
    const requiredKeys = ['chunkId', 'fileName', 'newFreeStorage'];
    const missingKeys = checkRequiredKeys(req.body, requiredKeys);

    if (missingKeys.length > 0) {
        return res.status(400).json({
            success: false,
            message: `Missing required parameters: ${missingKeys.join(', ')}.`
        });
    }
    const { chunkId, fileName, newFreeStorage } = req.body;
    const nodeIP = req.ip;
    addChunkToFile(FILES_FILE_PATH, fileName, chunkId, nodeIP, newFreeStorage);

    res.status(200).json({ success: true, message: `Chunk ${chunkId} added to file ${fileName}.` });
});

function addChunkToFile(filePath, fileName, chunkId, nodeIP, newFreeStorage) {
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
    updateNodeStorageCapacity(nodeIP, newFreeStorage);
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

app.get('/get-file', (req, res) => {
    const fileName = req.query.fileName;

    if (!fileName) {
        return res.status(400).json({ error: "fileName query parameter is required" });
    }

    const fileInfo = getFileInfo(FILES_FILE_PATH, fileName);
    if (fileInfo) {
        if (fileInfo.error) {
            return res.status(202).json(fileInfo);
        }
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
            if ("chunkSize" in files[fileName] && "totalChunks" in files[fileName]) {
                return files[fileName];
            } else {
                return { 
                    error: true, 
                    message: `File ${fileName} is currently being uploaded.` 
                };
            }
        }
    }
    return null;
}

app.get('/get-nodes', (req, res) => {
    const nodesData = getOnlineNodes(NODES_FILE_PATH);
    res.status(200).json(nodesData);
});

function getOnlineNodes(filePath) {
    let onlineNodes = {};
    if (fileExists(filePath)) {
        const nodesData = fs.readFileSync(filePath, 'utf8');
        const nodes = JSON.parse(nodesData);
        for (const [ip, nodeInfo] of Object.entries(nodes)) {
            if (nodeInfo.online) {
                onlineNodes[ip] = nodeInfo;
            }
        }
    }
    return onlineNodes;
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