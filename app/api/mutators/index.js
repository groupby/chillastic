const express    = require('express');
const controller = require('./mutators.controller');

const router = express.Router();
router.get('/:namespace', controller.getAllIdsByNamespace);
router.post('/:namespace/:id', controller.add);
router.delete('/:namespace/:id', controller.delete);

module.exports = () => router;
