require("dotenv").config();
const express = require("express");
const { Queue, Worker } = require("bullmq");
const IORedis = require("ioredis");
const app = express();
app.use(express.json());

// Create Redis connection
const connection = new IORedis({
	host: process.env.REDIS_HOST || "localhost",
	port: parseInt(process.env.REDIS_PORT || "6379"),
	maxRetriesPerRequest: null,
	retryStrategy: function (times) {
		return Math.min(Math.exp(times), 20000);
	},
});

console.log(
	`Connecting to Redis at ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`
);

// Create queues
const adminQueue = new Queue("admin-queue", { connection });
const condoQueue = new Queue("condo-queue", { connection });
const userQueue = new Queue("user-queue", { connection });

// Create workers for each queue
const adminWorker = new Worker(
	"admin-queue",
	async (job) => {
		console.log(`Processing admin job ${job.id}`);
		console.log("Admin job data:", job.data);

		try {
			const dashboardNotiKey = `dashboard:${condo_id}:${job.id}`;
			await connection.hset(dashboardNotiKey, {
				...job.data,
			});
		} catch (error) {
			await connection.hset(`job:${job.id}:tracking`, {
				status: "failed",
				error: error.message,
				failedAt: new Date().toISOString(),
			});

			// Set expiration on the error tracking data
			await connection.expire(`job:${job.id}:tracking`, 86400); // 24 hours

			throw error;
		}

		// Simulate admin work
		await new Promise((resolve) => setTimeout(resolve, 2000));

		console.log(`Admin job ${job.id} completed successfully!`);
		return { processed: true, type: "admin", jobId: job.id };
	},
	{ connection }
);

const userWorker = new Worker(
	"user-queue",
	async (job) => {
		console.log(`Processing user job ${job.id}`);
		console.log("User job data:", job.data);

		// Simulate user work
		await new Promise((resolve) => setTimeout(resolve, 1000));

		console.log(`User job ${job.id} completed successfully!`);
		return { processed: true, type: "user", jobId: job.id };
	},
	{ connection }
);

// Worker events
adminWorker.on("completed", (job) => {
	console.log(
		`Admin job ${job.id} has completed with result:`,
		job.returnvalue
	);
});

adminWorker.on("failed", (job, err) => {
	console.error(`Admin job ${job.id} has failed with error:`, err.message);
});

userWorker.on("completed", (job) => {
	console.log(
		`User job ${job.id} has completed with result:`,
		job.returnvalue
	);
});

userWorker.on("failed", (job, err) => {
	console.error(`User job ${job.id} has failed with error:`, err.message);
});

// API routes
app.post("/admin/jobs", async (req, res) => {
	try {
		const job = await adminQueue.add("admin-job", req.body);
		res.json({ success: true, jobId: job.id });
	} catch (error) {
		console.error("Failed to add admin job:", error);
		res.status(500).json({ error: error.message });
	}
});

app.post("/user/jobs", async (req, res) => {
	try {
		const job = await userQueue.add("user-job", req.body);
		res.json({ success: true, jobId: job.id });
	} catch (error) {
		console.error("Failed to add user job:", error);
		res.status(500).json({ error: error.message });
	}
});

app.get("/admin/jobs/:id", async (req, res) => {
	try {
		const job = await adminQueue.getJob(req.params.id);
		if (!job) {
			return res.status(404).json({ error: "Admin job not found" });
		}

		const state = await job.getState();
		console.log({ state, job });
		res.json({
			id: job.id,
			data: job.data,
			state: state,
			result: job.returnvalue,
		});
	} catch (error) {
		console.error(`Failed to get admin job ${req.params.id}:`, error);
		res.status(500).json({ error: error.message });
	}
});

app.get("/user/jobs/:id", async (req, res) => {
	try {
		const job = await userQueue.getJob(req.params.id);
		if (!job) {
			return res.status(404).json({ error: "User job not found" });
		}

		const state = await job.getState();
		res.json({
			id: job.id,
			data: job.data,
			state: state,
			result: job.returnvalue,
		});
	} catch (error) {
		console.error(`Failed to get user job ${req.params.id}:`, error);
		res.status(500).json({ error: error.message });
	}
});

app.get("/", async (req, res) => {
	res.send("Hello from server testing with queuebull");
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
	console.log(`Server running on port ${PORT}`);
	console.log("Workers started and waiting for jobs...");
});
