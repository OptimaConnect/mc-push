interface JQuery {
    emojioneArea: (options?: emojioneAreaOptions) => void
}

type emojioneAreaOptions = {
    pickerPosition: string,
    filtersPosition: string,
    tones: boolean,
    autocomplete: boolean,
    inline: boolean,
    events: {
        keyup: (editor: any, event: any) => void
    }
}