# Minimal Emoji Fix Script for Streamlit
# Run this to fix emoji encoding issues without changing your layout

def fix_emojis_in_file(file_path):
    '''
    Minimal fix for Streamlit emoji encoding issues
    Only replaces malformed emojis - keeps all your layout intact
    '''

    # Mapping of malformed emojis to correct ones
    emoji_fixes = [
        ('Ã°Å¸Å¡â‚¬', 'ðŸš€'),  # rocket
        ('Ã°Å¸"Å ', 'ðŸ“Š'),  # bar chart  
        ('Ã°Å¸'Â°', 'ðŸ’°'),  # money bag
        ('Ã°Å¸"â€¹', 'ðŸ“‹'),  # clipboard
        ('Ã°Å¸"Ë†', 'ðŸ“ˆ'),  # chart increasing
        ('Ã°Å¸"Â§', 'ðŸ”§'),  # wrench
        ('Ã°Å¸â€ â€¢', 'ðŸ†•'),  # new
        ('Ã°Å¸"', 'ðŸ“'),   # folder
        ('Ã¢Â°', 'â°'),    # alarm clock
        ('Ã¢Å“â€¦', 'âœ…'),    # check mark
        ('Ã¢Å’', 'âŒ'),     # cross mark
        ('Ã¢Å¡ Ã¯Â¸', 'âš ï¸'),    # warning
        ('Ã°Å¸"â€ž', 'ðŸ”„'),  # counterclockwise arrows
        ('Ã°Å¸Å¸Â¢', 'ðŸŸ¢'),  # green circle
        ('Ã°Å¸"Â´', 'ðŸ”´'),  # red circle
        ('Ã°Å¸Â¢', 'ðŸ¢'),   # office building
        ('Ã°Å¸Å’', 'ðŸŒ'),   # earth globe
        ('Ã¢â‚¬Â¢', 'â€¢'),     # bullet point
        ('Ã°Å¸"', 'ðŸ”'),   # magnifying glass
        ('Ã°Å¸'Â³', 'ðŸ’³'),  # credit card
        ('Ã°Å¸"â€¦', 'ðŸ“…'),  # calendar
        ('Ã°Å¸â€ºÃ¯Â¸', 'ðŸ›ï¸'),  # classical building
        ('Ã°Å¸"â€š', 'ðŸ“‚'),  # open file folder
        ('Ã°Å¸â€¢', 'ðŸ•'),   # clock
    ]

    try:
        # Read your current file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content

        # Fix each malformed emoji
        for bad_emoji, good_emoji in emoji_fixes:
            content = content.replace(bad_emoji, good_emoji)

        # Only write if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"âœ… Fixed emoji encoding in {file_path}")

            # Show what was fixed
            fixes_made = 0
            for bad_emoji, good_emoji in emoji_fixes:
                count = original_content.count(bad_emoji)
                if count > 0:
                    fixes_made += count
                    print(f"  Fixed {count}x: '{bad_emoji}' â†’ '{good_emoji}'")

            print(f"Total fixes applied: {fixes_made}")
        else:
            print("No emoji encoding issues found - file is already clean!")

    except Exception as e:
        print(f"Error: {e}")

# Usage - just run this:
if __name__ == "__main__":
    # Replace 'streamlit_app.py' with your actual filename
    fix_emojis_in_file('streamlit_app.py')
