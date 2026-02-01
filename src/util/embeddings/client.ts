import axios from "axios";
import sharp from "sharp";
import { databasePool } from "../database/connection-pool";

interface IdentificationMatch {
    flash_id: number;
    flash_name: string;
    similarity: number;
    confidence: number;
}

interface IdentificationResponse {
    success: boolean;
    image_processed: boolean;
    matches: IdentificationMatch[];
    timing_ms: number;
}

const EMBEDDINGS_API_URL = process.env.EMBEDDINGS_API_URL;
const RESIZE_MAX_DIMENSION = 384; // CLIP uses 224x224, so 384 gives plenty of detail

/**
 * Client for the invaders.embeddings flash identification API
 */
export class EmbeddingsClient {
    private enabled: boolean;

    constructor() {
        this.enabled = !!EMBEDDINGS_API_URL;
        if (this.enabled) {
            console.log(
                `[EmbeddingsClient] Initialized with API URL: ${EMBEDDINGS_API_URL}`,
            );
        } else {
            console.log(
                "[EmbeddingsClient] EMBEDDINGS_API_URL not set - flash identification disabled",
            );
        }
    }

    /**
     * Identify a flash from image data and store the top match
     * Stores regardless of confidence - filtering happens in invaders.bot
     * This is a non-blocking operation - failures are logged but don't interrupt processing
     */
    async identifyAndStore(
        sourceIpfsCid: string,
        imageData: Buffer,
    ): Promise<void> {
        if (!this.enabled) return;

        try {
            // Resize image before sending to reduce payload size
            const resizedImage = await this.resizeImage(imageData);

            const match = await this.identify(resizedImage);
            if (match) {
                await this.storeIdentification(sourceIpfsCid, match);
                console.log(
                    `[EmbeddingsClient] Identified ${sourceIpfsCid} as ${match.flash_name} (${(match.confidence * 100).toFixed(1)}% confidence)`,
                );
            }
        } catch (error) {
            const errorMsg =
                error instanceof Error ? error.message : "Unknown error";
            console.warn(
                `[EmbeddingsClient] Flash identification failed for ${sourceIpfsCid}: ${errorMsg}`,
            );
        }
    }

    /**
     * Resize image to reduce payload size while maintaining quality for CLIP
     * Resizes to max 384px on longest edge, preserving aspect ratio
     */
    private async resizeImage(imageData: Buffer): Promise<Buffer> {
        try {
            return await sharp(imageData)
                .resize(RESIZE_MAX_DIMENSION, RESIZE_MAX_DIMENSION, {
                    fit: "inside", // Preserve aspect ratio, fit within bounds
                    withoutEnlargement: true, // Don't upscale small images
                })
                .jpeg({ quality: 85 }) // Good balance of size vs quality
                .toBuffer();
        } catch (error) {
            // If resize fails, fall back to original image
            console.warn(
                `[EmbeddingsClient] Image resize failed, using original:`,
                error instanceof Error ? error.message : "Unknown error",
            );
            return imageData;
        }
    }

    /**
     * Call the embeddings API to identify an image
     */
    private async identify(
        imageData: Buffer,
    ): Promise<IdentificationMatch | null> {
        const formData = new FormData();
        const file = new File([new Uint8Array(imageData)], "image.jpg", {
            type: "image/jpeg",
        });
        formData.append("file", file);

        const response = await axios.post<IdentificationResponse>(
            `${EMBEDDINGS_API_URL}/identify?top_k=1`,
            formData,
            {
                headers: {
                    "Content-Type": "multipart/form-data",
                },
                timeout: 30000,
            },
        );

        if (
            response.data.success &&
            response.data.matches &&
            response.data.matches.length > 0
        ) {
            return response.data.matches[0];
        }

        return null;
    }

    /**
     * Store an identification result in the database
     */
    private async storeIdentification(
        sourceIpfsCid: string,
        match: IdentificationMatch,
    ): Promise<void> {
        await databasePool.query(
            `INSERT INTO flash_identifications
             (source_ipfs_cid, matched_flash_id, matched_flash_name, similarity, confidence)
             VALUES ($1, $2, $3, $4, $5)`,
            [
                sourceIpfsCid,
                match.flash_id,
                match.flash_name,
                match.similarity,
                match.confidence,
            ],
        );
    }

    /**
     * Check if the embeddings API is healthy
     */
    async healthCheck(): Promise<boolean> {
        if (!this.enabled) return true;

        try {
            const response = await axios.get(`${EMBEDDINGS_API_URL}/health`, {
                timeout: 5000,
            });
            return response.data.status === "healthy";
        } catch {
            return false;
        }
    }
}

// Export singleton instance
export const embeddingsClient = new EmbeddingsClient();
