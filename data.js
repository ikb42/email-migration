const mongoose = require('mongoose');

const TransferredSchema = new mongoose.Schema({
    email: String,
    msgIds: [],
    mailboxes: {},
    
    finalResult: {},
    updatedDate: { type: Date, default: Date.now },
    createdDate: { type: Date, default: Date.now }
});

TransferredSchema.methods = {
    saveDoc: function() {
        this.updatedDate = new Date()
        this.save()
    }
}


mongoose.connect('mongodb://localhost/email-transferred');
mongoose.connection.on('error', err => {
    console.error('MongoDB connection error: ' + err);
    process.exit(-1);
});


module.exports = mongoose.model('transferred', TransferredSchema);
